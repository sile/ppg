%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Plumtree Peer Process
%% @private
-module(ppg_peer).

-behaviour(gen_server).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/4]).
-export([stop/1]).
-export([stop_all/1]).
-export([is_valid_options/1]).
-export([get_graph/2]).
-export([get_destination/1]).
-export([broadcast/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(PG2_NAME(Name), {ppg_contact_peer, Name}). % TODO: Eliminate redundant definitions

-define(STATE, ?MODULE).

-record(opt,
        {
          contact_process_count :: pos_integer()
        }).

-record(?STATE,
        {
          group :: ppg:name(),
          destination :: pid(),
          pss :: ppg_peer_sampling_service:instance(),
          tree :: ppg_plumtree:tree(),
          opt :: #opt{}
        }).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(local:otp_name(), ppg:name(), pid(), ppg:join_options()) -> {ok, pid()} | {error, Reason} when
      Reason :: {no_such_group, ppg:name()}.
start_link(Name, Group, Destination, Options) ->
    gen_server:start_link(Name, ?MODULE, [Group, Destination, Options], []).

-spec stop(local:otp_ref()) -> ok.
stop(Peer) ->
    gen_server:stop(Peer).

-spec stop_all(local:otp_ref()) -> no_return().
stop_all(Peer) ->
    error(unimplemented, [Peer]).

-spec is_valid_options([ppg:join_option() | term()]) -> boolean().
is_valid_options(Options0) ->
    Options1 = Options0 ++ ppg:default_join_options(),
    Val = fun (Key) -> proplists:get_value(Key, Options1) end,
    (ppg_util:is_pos_integer(Val(contact_process_count))).

-spec broadcast(local:otp_ref(), term()) -> ok.
broadcast(Peer, Message) ->
    gen_server:call(Peer, {broadcast, Message}).

-spec get_destination(local:otp_ref()) -> pid().
get_destination(Peer) ->
    gen_server:call(Peer, get_destination).

-spec get_graph(local:otp_ref(), timeout()) -> ppg:communication_graph().
get_graph(Peer, Timeout) ->
    Ref0 = make_ref(),
    Parent = self(),
    {_, Monitor} =
        spawn_monitor(
          fun () ->
                  Ref1 = make_ref(),
                  From = {Ref1, self()},
                  _ = Timeout =:= infinity orelse erlang:send_after(Timeout, self(), timeout),
                  Entry = gen_server:call(Peer, {get_graph, From}),
                  Graph = build_graph([Entry], Ref1),
                  Parent ! {Ref0, Graph}
          end),
    receive
        {Ref0, Graph} -> Graph;
        {'DOWN', Monitor, _, _, Reason} -> exit(Reason)
    end.

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([Group, Destination, Options]) ->
    Opt = ppg_util:proplist_to_record(opt, record_info(fields, opt), Options),
    case become_contact_peer_if_needed(Group, Opt#opt.contact_process_count) of
        error -> {stop, {no_such_group, Group}};
        ok    ->
            _ = link(Destination),
            _ = monitor(process, Destination),

            ContactPeer = pg2:get_closest_pid(?PG2_NAME(Group)),
            Pss0 = proplists:get_value(peer_sampling_service, Options, ppg:default_peer_sampling_service()),
            Pss1 = ppg_peer_sampling_service:join(ContactPeer, Pss0),
            {Peers, Pss2} = ppg_peer_sampling_service:get_peers(Pss1),
            Tree = ppg_plumtree:new(Destination, Peers),

            State =
                #?STATE{
                    group = Group,
                    destination = Destination,
                    pss = Pss2,
                    tree = Tree,
                    opt = Opt
                   },
            {ok, State}
    end.

%% @private
handle_call({broadcast, Arg}, _From, State) ->
    handle_broadcast(Arg, State);
handle_call(get_destination, _From, State) ->
    {reply, State#?STATE.destination, State};
handle_call({get_graph, Arg}, _From, State) ->
    handle_get_graph(Arg, State);
handle_call(_Request, _From, State) ->
    {noreply, State}.

%% @private
handle_cast(_Request, State) ->
    {noreply, State}.

%% @private
handle_info(Info, State) ->
    case ppg_peer_sampling_service:handle_info(Info, State#?STATE.pss) of
        {ok, Pss}       -> {noreply, State#?STATE{pss = Pss}};
        {error, Reason} -> {stop, Reason, State};
        ignore          ->
            case ppg_plumtree:handle_info(Info, State#?STATE.tree) of
                {ok, Tree}      -> {noreply, State#?STATE{tree = Tree}};
                {error, Reason} -> {stop, Reason, State};
                ignore          ->
                    case Info of
                        {'DOWN', _, _, Pid, _} when Pid =:= State#?STATE.destination ->
                            {stop, normal, State};
                        _ ->
                            {stop, {unknown_info, Info}, State}
                            %% {noreply, State} TODO:
                    end
            end
    end.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec become_contact_peer_if_needed(ppg:name(), pos_integer()) -> ok | error.
become_contact_peer_if_needed(Group, MinContactProcCount) ->
    case pg2:get_members(?PG2_NAME(Group)) of
        {error, _} -> error;
        Peers      ->
            _ = length(Peers) < MinContactProcCount andalso pg2:join(?PG2_NAME(Group), self()),
            ok
    end.

%% TODO: 未接続のエッジがなくなったらタイムアウトを待たずに終了する (接続性が満たされているかどうかを返すのも良いかもしれない)
-spec build_graph(ppg:communication_graph(), reference()) -> ppg:communication_graph().
build_graph(Acc, Ref) ->
    receive
        {Ref, Entry} -> build_graph([Entry | Acc], Ref);
        timeout      -> Acc
    end.

-spec handle_broadcast(term(), #?STATE{}) -> {reply, ok, #?STATE{}}.
handle_broadcast(Message, State) ->
    Tree = ppg_plumtree:broadcast(Message, State#?STATE.tree),
    {reply, ok, State#?STATE{tree = Tree}}.

-spec handle_get_graph({reference(), pid()}, #?STATE{}) -> {reply, Todo::term(), #?STATE{}}.
handle_get_graph(_Requestor, State) ->
    Entry = {self(), State#?STATE.destination, []},
    {reply, Entry, State}.
