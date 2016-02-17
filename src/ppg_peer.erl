%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Plumtree/HyParView Peer Process
%% @private
-module(ppg_peer).

-behaviour(gen_server).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/3]).
-export([stop/1]).
-export([broadcast/2]).
-export([get_member/1]).
-export([get_graph/2]).

-export_type([graph/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%----------------------------------------------------------------------------------------------------------------------
%% Internal API
%%----------------------------------------------------------------------------------------------------------------------
-export([build_graph/3]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(STATE, ?MODULE).

-record(?STATE,
        {
          view :: ppg_hyparview:view(),
          tree :: ppg_plumtree:tree()
        }).

-type graph() :: [{ppg:peer(), ppg:member(), [ppg_plumtree:peer()]}].

-type from() :: {reference(), pid()}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(ppg:name(), ppg:member(), ppg:join_options()) -> {ok, pid()} | {error, Reason::term()}.
start_link(Group, Member, Options) ->
    gen_server:start_link(?MODULE, [Group, Member, Options], []).

-spec stop(ppg:peer()) -> ok.
stop(Peer) ->
    gen_server:stop(Peer).

-spec broadcast(ppg:peer(), ppg:message()) -> ok.
broadcast(Peer, Message) ->
    gen_server:call(Peer, {broadcast, Message}).

-spec get_member(ppg:peer()) -> ppg:member().
get_member(Peer) ->
    gen_server:call(Peer, get_member).

-spec get_graph(ppg:peer(), timeout()) -> graph().
get_graph(Peer, Timeout) ->
    Tag = make_ref(),
    {_, Monitor} = spawn_monitor(?MODULE, build_graph, [{Tag, self()}, Peer, Timeout]),
    receive
        {Tag, Graph} ->
            _ = demonitor(Monitor, [flush]),
            Graph;
        {'DOWN', Monitor, _, _, Reason} ->
            exit(Reason)
    end.

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([Group, Member, Options]) ->
    _ = monitor(process, Member),

    View0 = ppg_hyparview:new(Group, proplists:get_value(hyparview, Options, [])),
    View1 = ppg_hyparview:join(View0),
    Tree  = ppg_plumtree:new(Member, ppg_hyparview:get_peers(View1), proplists:get_value(plumtree, Options, [])),

    State = #?STATE{view = View1, tree = Tree},
    {ok, State}.

%% @private
handle_call({broadcast, Arg}, _From, State) -> handle_broadcast(Arg, State);
handle_call(get_member,       _From, State) -> handle_get_member(State);
handle_call(_Request,         _From, State) -> {noreply, State}.

%% @private
handle_cast({collect_peer_info, Arg}, State) -> handle_collect_peer_info(Arg, State);
handle_cast(_Request, State)                 -> {noreply, State}.

%% @private
handle_info(Info, State) ->
    case handle_hyparview_info(Info, State) of
        ignore ->
            case handle_plumtree_info(Info, State) of
                ignore -> handle_peer_info(Info, State);
                Other  -> Other
            end;
        Other -> Other
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
-spec handle_hyparview_info(term(), #?STATE{}) -> {noreply, #?STATE{}} | ignore.
handle_hyparview_info(Info, State) ->
    case ppg_hyparview:handle_info(Info, State#?STATE.view) of
        ignore      -> ignore;
        {ok, View0} ->
            {Queue, View1} = ppg_hyparview:flush_queue(View0),
            Tree =
                lists:foldl(
                  fun ({up, Peer, Conn},  Acc) -> ppg_plumtree:neighbor_up(Peer, Conn, Acc);
                      ({down, Peer,Conn}, Acc) -> ppg_plumtree:neighbor_down(Peer, Conn, Acc);
                      ({broadcast, Msg},  Acc) -> ppg_plumtree:system_broadcast(Msg, Acc)
                  end,
                  State#?STATE.tree,
                  Queue),
            {noreply, State#?STATE{view = View1, tree = Tree}}
    end.

-spec handle_plumtree_info(term(), #?STATE{}) -> {noreply, #?STATE{}} | ignore.
handle_plumtree_info(Info, State) ->
    case ppg_plumtree:handle_info(Info, State#?STATE.tree) of
        ignore     -> ignore;
        {ok, Tree} -> {noreply, State#?STATE{tree = Tree}}
    end.

-spec handle_peer_info(term(), #?STATE{}) -> {noreply, #?STATE{}} | {stop, Reason::term(), #?STATE{}}.
handle_peer_info(Info, State) ->
    Tree = State#?STATE.tree,
    Member = ppg_plumtree:get_member(Tree),
    case Info of
        {'DOWN', _, _, Pid, _} when Pid =:= Member ->
            {stop, normal, State};
        {?MODULE, get_peer_info, From} ->
            ok = reply(From, {self(), Member, ppg_plumtree:get_peers(Tree)}),
            {noreply, State};
        _ ->
            {stop, {unknown_info, Info}, State}
    end.

-spec handle_broadcast(ppg:message(), #?STATE{}) -> {reply, ok, #?STATE{}}.
handle_broadcast(Message, State) ->
    Tree = ppg_plumtree:broadcast(Message, State#?STATE.tree),
    {reply, ok, State#?STATE{tree = Tree}}.

-spec handle_get_member(#?STATE{}) -> {reply, ppg:member(), #?STATE{}}.
handle_get_member(State) ->
    {reply, ppg_plumtree:get_member(State#?STATE.tree), State}.

-spec handle_collect_peer_info(from(), #?STATE{}) -> {noreply, #?STATE{}}.
handle_collect_peer_info(From, State) ->
    Tree = ppg_plumtree:system_broadcast({?MODULE, get_peer_info, From}, State#?STATE.tree),
    {noreply, State#?STATE{tree = Tree}}.

-spec reply(from(), term()) -> ok.
reply({Tag, Pid}, Message) ->
    _ = Pid ! {Tag, Message},
    ok.

%% @private
-spec build_graph(from(), ppg:peer(), timeout()) -> ok.
build_graph(From, Peer, Timeout) when is_integer(Timeout) ->
    _ = erlang:send_after(Timeout, self(), timeout),
    build_graph(From, Peer, infinity);
build_graph(From, Peer, _) ->
    Tag = make_ref(),
    ok = gen_server:cast(Peer, {collect_peer_info, {Tag, self()}}),
    Graph = receive_graph(Tag, [], gb_sets:empty()),
    reply(From, Graph).

-spec receive_graph(reference(), graph(), gb_sets:set(Edge)) -> graph() when
      Edge :: ppg_hyparview:connection().
receive_graph(Tag, Acc, UnknownEdges0) ->
    receive
        timeout      -> Acc;
        {Tag, Entry} ->
            {_, _, Peers} = Entry,
            UnknownEdges1 =
                lists:foldl(
                  fun (#{connection := Edge}, AccEdges) ->
                          case gb_sets:is_member(Edge, AccEdges) of
                              false -> gb_sets:add(Edge, AccEdges);
                              true  -> gb_sets:delete(Edge, AccEdges)
                          end
                  end,
                  UnknownEdges0,
                  Peers),
            case gb_sets:is_empty(UnknownEdges1) of
                true  -> [Entry | Acc]; % HyParViewの性質上、未知の辺がないなら全てのピアが探索済みだと分かる (quiescentな状態なら)
                false -> receive_graph(Tag, [Entry | Acc], UnknownEdges1)
            end
    end.
