%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Plumtree Peer Process
%% @private
-module(ppg_peer).

-behaviour(gen_server).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/3]).
-export([stop/1]).
-export([get_graph/2]).
-export([get_destination/1]).
-export([broadcast/2]).

-export_type([peer/0]).
-export_type([contact_peer/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(STATE, ?MODULE).

-record(?STATE,
        {
          group :: ppg:name(),
          destination :: pid(), % TODO: rename
          view :: ppg_hyparview:view(),
          tree :: ppg_plumtree:tree()
        }).

-type peer() :: pid().
-type contact_peer() :: peer().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(local:otp_name(), ppg:name(), ppg:member()) -> {ok, pid()} | {error, Reason::term()}.
start_link(Name, Group, Member) ->
    gen_server:start_link(Name, ?MODULE, [Group, Member], []).

-spec stop(local:otp_ref()) -> ok.
stop(Peer) ->
    gen_server:stop(Peer).

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
                  ok = gen_server:cast(Peer, {get_graph, From}),
                  Graph = build_graph([], Ref1),
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
init([Group, Member]) ->
    _ = link(Member),
    _ = monitor(process, Member),

    View0 = ppg_hyparview:new(Group),
    View1 = ppg_hyparview:join(View0),
    Tree = ppg_plumtree:new(Member, ppg_hyparview:get_peers(View1)),

    State =
        #?STATE{
            group = Group,
            destination = Member,
            view = View1,
            tree = Tree
           },
    {ok, State}.

%% @private
handle_call({broadcast, Arg}, _From, State) ->
    handle_broadcast(Arg, State);
handle_call(get_destination, _From, State) ->
    {reply, State#?STATE.destination, State};
handle_call(_Request, _From, State) ->
    {noreply, State}.

%% @private
handle_cast({get_graph, Arg}, State) ->
    handle_get_graph(Arg, State);
handle_cast(_Request, State) ->
    {noreply, State}.

%% @private
handle_info(Info, State) ->
    io:format("# [~p] ~w\n", [self(), Info]),
    case ppg_hyparview:handle_info(Info, State#?STATE.view) of
        {ok, View} ->
            {Queue, View1} = ppg_hyparview:flush_queue(View),
            Tree =
                lists:foldl(
                  fun ({up, Peer, Conn},  Acc) -> ppg_plumtree:neighbor_up(Peer, Conn, Acc);
                      ({down, Peer,Conn}, Acc) -> ppg_plumtree:neighbor_down(Peer, Conn, Acc);
                      ({broadcast, Msg},  Acc) -> ppg_plumtree:system_broadcast(Msg, Acc)
                  end,
                  State#?STATE.tree,
                  Queue),
            {noreply, State#?STATE{view = View1, tree = Tree}};
        ignore     ->
            case ppg_plumtree:handle_info(Info, State#?STATE.tree) of
                {ok, Tree} -> {noreply, State#?STATE{tree = Tree}};
                ignore     ->
                    case Info of
                        {'DOWN', _, _, Pid, _} when Pid =:= State#?STATE.destination ->
                            {stop, normal, State};
                        {?MODULE, get_graph, {Tag, From}} ->
                            Tree = State#?STATE.tree,
                            _ = From ! {Tag, {self(), ppg_plumtree:get_member(Tree), ppg_plumtree:get_peers(Tree)}},
                            {noreply, State};
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

-spec handle_get_graph({reference(), pid()}, #?STATE{}) -> {noreply, #?STATE{}}.
handle_get_graph(Requestor, State) ->
    Tree = ppg_plumtree:system_broadcast({?MODULE, get_graph, Requestor}, State#?STATE.tree),
    {noreply, State#?STATE{tree = Tree}}.
