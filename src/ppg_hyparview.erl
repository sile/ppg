%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc A Peer Sampling Service Implementation based HyParView Algorithm
%%
%% @reference See http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf
%% @private
-module(ppg_hyparview).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([default_options/0]).

-export([new/2]).
-export([join/2]).
-export([disconnect/3]).
-export([handle_info/3]).

-export_type([view/0]).
-export_type([connection/0]).
-export_type([tree/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(VIEW, ?MODULE).
-record(?VIEW,
        {
          %% Peers(View) Management
          active_view  = #{} :: #{ppg_peer:peer() => connection()},
          passive_view = #{} :: #{ppg_peer:peer() => ok},
          monitors     = #{} :: #{ppg_peer:peer() => reference()},

          %% Timers
          shuffle_timer      = make_ref() :: reference(),
          connectivity_timer = make_ref() :: reference(),
          rejoin_timer       = make_ref() :: reference(),

          %% Contact Service
          contact_service :: ppg_contact_service:service(),

          %% Protocol Parameters
          active_view_size               :: pos_integer(),
          passive_view_size              :: pos_integer(),
          active_random_walk_length      :: pos_integer(),
          passive_random_walk_length     :: pos_integer(),
          shuffle_count                  :: pos_integer(),
          shuffle_interval               :: timeout(),
          max_broadcast_delay            :: timeout(),
          allowable_disconnection_period :: timeout()
        }).

-opaque view() :: #?VIEW{}.
%% A HyParView instance

-opaque connection() :: reference().
%% An identifier of a logical connection between two peers

-type ttl() :: non_neg_integer().
%% Time To Live

-type tree() :: ppg_plumtree:tree().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec default_options() -> [ppg:hyparview_option()].
default_options() ->
    %% NOTE: Below default values may be sufficient for a group which have less than one thousand members.
    [
     {active_view_size, 4},
     {passive_view_size, 20},
     {active_random_walk_length, 5},
     {passive_random_walk_length, 2},
     {shuffle_count, 4},
     {shuffle_interval, 10 * 60 * 1000},
     {max_broadcast_delay, 10 * 1000},
     {allowable_disconnection_period, 60 * 1000}
    ].

-spec new(ppg:name(), [ppg:hyparview_option()]) -> view().
new(Group, Options0) ->
    _ = is_list(Options0) orelse error(badarg, [Options0]),
    Options1 = Options0 ++ default_options(),

    Get =
        fun (Key, Validate) ->
                Value = proplists:get_value(Key, Options1),
                _ = Validate(Value) orelse error(badarg, [Options0]),
                Value
        end,
    View =
        #?VIEW{
            contact_service = ppg_contact_service:new(Group),
            active_view_size = Get(active_view_size, fun ppg_util:is_pos_integer/1),
            passive_view_size = Get(passive_view_size, fun ppg_util:is_pos_integer/1),
            active_random_walk_length = Get(active_random_walk_length, fun ppg_util:is_pos_integer/1),
            passive_random_walk_length = Get(passive_random_walk_length, fun ppg_util:is_pos_integer/1),
            shuffle_count = Get(shuffle_count, fun ppg_util:is_pos_integer/1),
            shuffle_interval = Get(shuffle_interval, fun ppg_util:is_timeout/1),
            max_broadcast_delay = Get(max_broadcast_delay, fun ppg_util:is_timeout/1),
            allowable_disconnection_period = Get(allowable_disconnection_period, fun ppg_util:is_timeout/1)
           },
    schedule_shuffle(View).

-spec join(tree(), view()) -> {tree(), view()}.
join(Tree, View) ->
    ContactPeer = ppg_contact_service:get_peer(View#?VIEW.contact_service),
    case ContactPeer =:= self() of
        true  -> {Tree, View};
        false ->
            _ = ContactPeer ! message_join(),
            %% Joined peer must be connected with at least one other peer (or timed out in the caller side)
            receive
                {'CONNECT', Arg} -> handle_connect(Arg, Tree, View)
            end
    end.

-spec disconnect(ppg_peer:peer(), tree(), view()) -> {tree(), view()}.
disconnect(Peer, Tree0, View0) ->
    {ok, Tree1, View1} = disconnect_peer(Peer, undefined, Tree0, View0),
    {Tree1, View1}.

-spec handle_info(term(), tree(), view()) -> {ok, {tree(), view()}} | ignore.
handle_info({'JOIN',         Arg},    Tree, View) -> {ok, handle_join(Arg, Tree, View)};
handle_info({'FORWARD_JOIN', Arg},    Tree, View) -> {ok, handle_forward_join(Arg, Tree, View)};
handle_info({'CONNECT',      Arg},    Tree, View) -> {ok, handle_connect(Arg, Tree, View)};
handle_info({'DISCONNECT',   Arg},    Tree, View) -> {ok, handle_disconnect(Arg, false, Tree, View)};
handle_info({'NEIGHBOR',     Arg},    Tree, View) -> {ok, handle_neighbor(Arg, Tree, View)};
handle_info({'FOREIGNER',    Arg},    Tree, View) -> {ok, handle_foreigner(Arg, Tree, View)};
handle_info({'SHUFFLE',      Arg},    Tree, View) -> {ok, {Tree, handle_shuffle(Arg, View)}};
handle_info({'SHUFFLEREPLY', Arg},    Tree, View) -> {ok, {Tree, handle_shufflereply(Arg, View)}};
handle_info({'CONNECTIVITY', Arg},    Tree, View) -> {ok, handle_connectivity(Arg, Tree, View)};
handle_info({?MODULE, start_shuffle}, Tree, View) -> {ok, handle_start_shuffle(Tree, View)};
handle_info({?MODULE, rejoin},        Tree, View) -> {ok, join(Tree, View)};
handle_info({'DOWN', Ref, _, Pid, _}, Tree, View) ->
    case View of
        #?VIEW{monitors = #{Pid := Ref}} ->
            case View of
                #?VIEW{active_view = #{Pid := Conn}} -> {ok, handle_disconnect({Conn, Pid}, true, Tree, View)};
                #?VIEW{passive_view = #{Pid := _}}   -> {ok, handle_foreigner(Pid, Tree, View)}
            end;
        _ -> ignore
    end;
handle_info(_Info, _Tree, _View) -> ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_join(ppg_peer:peer(), tree(), view()) -> {tree(), view()}.
handle_join(NewPeer, Tree0, View0 = #?VIEW{active_random_walk_length = ARWL}) ->
    {Tree1, View1} = add_peer_to_active_view(NewPeer, undefined, Tree0, View0),
    ok = ppg_maps:foreach(
           fun (P, _) -> P =/= NewPeer andalso (P ! message_forward_join(NewPeer, ARWL)) end,
           View1#?VIEW.active_view),
    {Tree1, View1}.

-spec handle_forward_join({ppg_peer:peer(), ttl(), ppg_peer:peer()}, tree(), view()) -> {tree(), view()}.
handle_forward_join({NewPeer, 0, _}, Tree, View) ->
    add_peer_to_active_view(NewPeer, undefined, Tree, View);
handle_forward_join({NewPeer, _, _}, Tree, View = #?VIEW{active_view = Active}) when Active =:= #{} ->
    add_peer_to_active_view(NewPeer, undefined, Tree, View);
handle_forward_join({NewPeer, TimeToLive, Sender}, Tree, View) ->
    Next = ppg_maps:random_key(maps:remove(Sender, View#?VIEW.active_view), Sender),
    _ = Next ! message_forward_join(NewPeer, TimeToLive - 1),
    DoesAddToPassiveView =
        maps:size(View#?VIEW.passive_view) < View#?VIEW.passive_view_size orelse
        TimeToLive =:= View#?VIEW.passive_random_walk_length,
    case DoesAddToPassiveView of
        false -> {Tree, View};
        true  -> {Tree, add_peers_to_passive_view([NewPeer], View)}
    end.

-spec handle_connect({connection(), ppg_peer:peer()}, tree(), view()) -> {tree(), view()}.
handle_connect({Conn, Peer}, Tree, View) ->
    add_peer_to_active_view(Peer, Conn, Tree, View).

-spec handle_disconnect({connection(), ppg_peer:peer()}, boolean(), tree(), view()) -> {tree(), view()}.
handle_disconnect({Connection, Peer}, IsPeerDown, Tree0, View0) ->
    case disconnect_peer(Peer, Connection, Tree0, View0) of
        error              -> {Tree0, View0};
        {ok, Tree1, View1} ->
            {Tree2, View2} = promote_passive_peer_if_needed(Tree1, View1),
            View3 = start_connectivity_check_if_needed(View2),
            case IsPeerDown of
                true  -> {Tree2, View3};
                false -> {Tree2, add_peers_to_passive_view([Peer], View3)}
            end
    end.

-spec handle_neighbor({high|low, ppg_peer:peer()}, tree(), view()) -> {tree(), view()}.
handle_neighbor({high, Peer}, Tree, View) -> add_peer_to_active_view(Peer, undefined, Tree, View);
handle_neighbor({low,  Peer}, Tree, View) ->
    case maps:size(View#?VIEW.active_view) < View#?VIEW.active_view_size of
        true  -> add_peer_to_active_view(Peer, undefined, Tree, View);
        false -> _ = Peer ! message_foreigner(), {Tree, View}
    end.

-spec handle_foreigner(ppg_peer:peer(), tree(), view()) -> {tree(), view()}.
handle_foreigner(Peer, Tree, View0) ->
    %% For simplicity, we remove the peer which have rejected a NEIGHBOR request from the passive view
    %% (It is different behavior from the original paper)
    View1 = remove_passive_peer(Peer, View0),
    promote_passive_peer_if_needed(Tree, View1).

-spec handle_shuffle({[ppg_peer:peer()], ttl(), ppg_peer:peer()}, view()) -> view().
handle_shuffle({[Origin | _] = Peers, TimeToLive, Sender}, View) ->
    NextCandidates = maps:without([Origin, Sender], View#?VIEW.active_view),
    case {TimeToLive > 0, ppg_maps:random_key(NextCandidates)} of
        {true, {ok, Next}} ->
            _ = Next ! message_shuffle(Peers, TimeToLive - 1),
            View;
        _ ->
            ReplyPeers = ppg_maps:random_keys(length(Peers), View#?VIEW.passive_view),
            _ = Origin ! message_shufflereply(ReplyPeers),
            add_peers_to_passive_view(Peers, View)
    end.

-spec handle_shufflereply([ppg_peer:peer()], view()) -> view().
handle_shufflereply(Peers, View) ->
    add_peers_to_passive_view(Peers, View).

-spec start_connectivity_check_if_needed(view()) -> view().
start_connectivity_check_if_needed(View) ->
    ContactPeer = ppg_contact_service:get_peer(View#?VIEW.contact_service),
    case self() =:= ContactPeer of
        true  -> View;
        false ->
            %% FIXME: Replace to a more strict and scalable means for assurance of the connectivity of the graph
            case is_integer(erlang:read_timer(View#?VIEW.rejoin_timer)) of
                true  -> View;
                false ->
                    _ = ContactPeer ! message_connectivity(up),
                    After = View#?VIEW.allowable_disconnection_period,
                    Timer = erlang:send_after(After, self(), {?MODULE, rejoin}),
                    View#?VIEW{rejoin_timer = Timer}
            end
    end.

-spec schedule_shuffle(view()) -> view().
schedule_shuffle(View = #?VIEW{shuffle_interval = infinity}) ->
    View;
schedule_shuffle(View = #?VIEW{shuffle_interval = Interval}) ->
    After = (Interval div 2) + (rand:uniform(Interval + 1) - 1),
    Timer = ppg_util:cancel_and_send_after(View#?VIEW.shuffle_timer, After, self(), {?MODULE, start_shuffle}),
    View#?VIEW{shuffle_timer = Timer}.

-spec handle_start_shuffle(tree(), view()) -> {tree(), view()}.
handle_start_shuffle(Tree0, View0 = #?VIEW{shuffle_count = Count}) ->
    {Tree1, View1} =
        case is_small_active_view(View0) of
            false -> {Tree0, View0};
            true  -> promote_passive_peer_if_needed(Tree0, View0)
        end,
    ActiveCount = (Count + 1) div 2,
    Peers =
        [self()] ++
        ppg_maps:random_keys(ActiveCount - 1, View1#?VIEW.active_view) ++
        ppg_maps:random_keys(Count - ActiveCount, View1#?VIEW.passive_view),
    _ = case ppg_maps:random_key(View1#?VIEW.active_view) of
            error      -> ok;
            {ok, Next} -> Next ! message_shuffle(Peers, View1#?VIEW.active_random_walk_length)
        end,
    {Tree1, schedule_shuffle(View1)}.

-spec handle_connectivity(up|kick|down, tree(), view()) -> {tree(), view()}.
handle_connectivity(up, Tree, View) ->
    ContactPeer = ppg_contact_service:get_peer(View#?VIEW.contact_service),
    case ContactPeer =:= self() of
        false ->
            %% Forwards to the latest contact peer
            _ = ContactPeer ! message_connectivity(up),
            {Tree, View};
        true  ->
            case erlang:read_timer(View#?VIEW.connectivity_timer) =/= false of
                true  -> {Tree, View}; % The timer has been set
                false ->
                    After = max(0, View#?VIEW.allowable_disconnection_period - View#?VIEW.max_broadcast_delay),
                    Timer = erlang:send_after(After, self(), message_connectivity(kick)),
                    {Tree, View#?VIEW{connectivity_timer = Timer}}
            end
    end;
handle_connectivity(kick, Tree, View) ->
    {ppg_plumtree:system_broadcast(message_connectivity(down), Tree), View};
handle_connectivity(down, Tree, View) ->
    _ = ppg_util:cancel_and_flush_timer(View#?VIEW.rejoin_timer, {?MODULE, rejoin}),
    {Tree, View}.

-spec add_peer_to_active_view(ppg_peer:peer(), connection()|undefined, tree(), view()) -> {tree(), view()}.
add_peer_to_active_view(Peer,_Connection, Tree, View) when Peer =:= self() ->
    {Tree, View};
add_peer_to_active_view(Peer, Connection, Tree0, View0) ->
    case maps:find(Peer, View0#?VIEW.active_view) of
        error ->
            %% A new peer; We add the peer to the active view
            {Tree1, View1} = ensure_active_view_free_space(Tree0, View0),
            connect_to_peer(Peer, Connection, Tree1, View1);
        {ok, Existing} ->
            %% The peer already exists in the active view
            case Connection =:= undefined orelse Connection =< Existing of
                true  -> {Tree0, View0}; % `Existing' is up to date (ignores `Connection')
                false ->
                    %% `Existing' is out of date (implicitly reconnected)
                    Tree1 = ppg_plumtree:neighbor_down(Existing, Tree0),
                    Tree2 = ppg_plumtree:neighbor_up(Peer, Connection, Tree1),
                    {Tree2, View0#?VIEW{active_view = maps:put(Peer, Connection, View0#?VIEW.active_view)}}
            end
    end.

-spec ensure_active_view_free_space(tree(), view()) -> {tree(), view()}.
ensure_active_view_free_space(Tree0, View0) ->
    case maps:size(View0#?VIEW.active_view) < View0#?VIEW.active_view_size of
        true  -> {Tree0, View0};
        false ->
            {ok, Peer} = ppg_maps:random_key(View0#?VIEW.active_view),
            {ok, Tree1, View1} = disconnect_peer(Peer, undefined, Tree0, View0),
            View2 = add_peers_to_passive_view([Peer], View1),
            ensure_active_view_free_space(Tree1, View2)
    end.

-spec connect_to_peer(ppg_peer:peer(), connection()|undefined, tree(), view()) -> {tree(), view()}.
connect_to_peer(Peer, undefined, Tree, View) ->
    Connection = make_ref(),
    _ = Peer ! message_connect(Connection),
    connect_to_peer(Peer, Connection, Tree, View);
connect_to_peer(Peer, Connection, Tree0, View0) ->
    View1 = remove_passive_peer(Peer, View0),
    Tree1 = ppg_plumtree:neighbor_up(Peer, Connection, Tree0),
    ActiveView = maps:put(Peer, Connection, View1#?VIEW.active_view),
    Monitors = maps:put(Peer, monitor(process, Peer), View1#?VIEW.monitors),
    {Tree1, View1#?VIEW{active_view = ActiveView, monitors = Monitors}}.

-spec disconnect_peer(ppg_peer:peer(), connection()|undefined, tree(), view()) -> {ok, tree(), view()} | error.
disconnect_peer(Peer, undefined, Tree, View) ->
    Connection = maps:get(Peer, View#?VIEW.active_view),
    _ = Peer ! message_disconnect(Connection),
    disconnect_peer(Peer, Connection, Tree, View);
disconnect_peer(Peer, Connection, Tree0, View) ->
    case View#?VIEW.active_view of
        #{Peer := Connection} ->
            Tree1 = ppg_plumtree:neighbor_down(Connection, Tree0),
            _ = demonitor(maps:get(Peer, View#?VIEW.monitors), [flush]),
            Monitors = maps:remove(Peer, View#?VIEW.monitors),
            ActiveView = maps:remove(Peer, View#?VIEW.active_view),
            {ok, Tree1, View#?VIEW{active_view = ActiveView, monitors = Monitors}};
        _ ->
            %% The connection is already disconnected
            error
    end.

-spec remove_passive_peer(ppg_peer:peer(), view()) -> view().
remove_passive_peer(Peer, View) ->
    _ = demonitor(maps:get(Peer, View#?VIEW.monitors, make_ref()), [flush]),
    PassiveView = maps:remove(Peer, View#?VIEW.passive_view),
    View#?VIEW{passive_view = PassiveView}.

-spec add_peers_to_passive_view([ppg_peer:peer()], view()) -> view().
add_peers_to_passive_view(Peers0, View0) ->
    Peers1 = Peers0 -- [self() | maps:keys(View0#?VIEW.active_view)],
    View1 = View0#?VIEW{passive_view = maps:without(Peers1, View0#?VIEW.passive_view)}, % Temporary removed from the view
    View2 = ensure_passive_view_free_space(length(Peers1), View1),
    PassiveView = maps:merge(View2#?VIEW.passive_view, maps:from_list([{P, ok} || P <- Peers1])),
    View2#?VIEW{passive_view = PassiveView}.

-spec ensure_passive_view_free_space(pos_integer(), view()) -> view().
ensure_passive_view_free_space(Room, View) ->
    case maps:size(View#?VIEW.passive_view) =< max(0, View#?VIEW.passive_view_size - Room) of
        true  -> View;
        false ->
            {ok, Peer} = ppg_maps:random_key(View#?VIEW.passive_view),
            ensure_passive_view_free_space(Room - 1, remove_passive_peer(Peer, View))
    end.

-spec promote_passive_peer_if_needed(tree(), view()) -> {tree(), view()}.
promote_passive_peer_if_needed(Tree, View) ->
    ActivePeerCount = maps:size(View#?VIEW.active_view),
    case ActivePeerCount < View#?VIEW.active_view_size of
        false -> {Tree, View};
        true  ->
            %% NOTE: Removes peers in progress
            Candidates = maps:filter(fun (P, _) -> not maps:is_key(P, View#?VIEW.monitors) end, View#?VIEW.passive_view),
            case {ppg_maps:random_key(Candidates), ActivePeerCount} of
                {error,      0} -> join(Tree, View);
                {error,      _} -> {Tree, View};
                {{ok, Peer}, _} ->
                    _ = Peer ! message_neighbor(View),
                    Monitors = maps:put(Peer, monitor(process, Peer), View#?VIEW.monitors),
                    {Tree, View#?VIEW{monitors = Monitors}}
            end
    end.

-spec is_small_active_view(view()) -> boolean().
is_small_active_view(#?VIEW{active_view = ActiveView, active_view_size = Size}) ->
    maps:size(ActiveView) < max(1, Size div 2).

-spec message_join() -> {'JOIN', NewPeer::ppg_peer:peer()}.
message_join() ->
    {'JOIN', self()}.

-spec message_forward_join(NewPeer, TimeToLive) -> {'FORWARD_JOIN', {NewPeer, TimeToLive, Sender}} when
      NewPeer    :: ppg_peer:peer(),
      TimeToLive :: pos_integer(),
      Sender     :: ppg_peer:peer().
message_forward_join(NewPeer, TimeToLive) ->
    {'FORWARD_JOIN', {NewPeer, TimeToLive, self()}}.

-spec message_connect(connection()) -> {'CONNECT', {connection(), ppg_peer:peer()}}.
message_connect(Conn) ->
    {'CONNECT', {Conn, self()}}.

-spec message_disconnect(connection()) -> {'DISCONNECT', {connection(), ppg_peer:peer()}}.
message_disconnect(Conn) ->
    {'DISCONNECT', {Conn, self()}}.

-spec message_neighbor(view()) -> {'NEIGHBOR', {high|low, ppg_peer:peer()}}.
message_neighbor(View) ->
    Priority = case is_small_active_view(View) of true -> high; _ -> low end,
    {'NEIGHBOR', {Priority, self()}}.

-spec message_foreigner() -> {'FOREIGNER', ppg_peer:peer()}.
message_foreigner() ->
    {'FOREIGNER', self()}.

-spec message_shuffle(Peers, pos_integer()) -> {'SHUFFLE', {Peers, pos_integer(), ppg_peer:peer()}} when
      Peers :: [ppg_peer:peer()].
message_shuffle(Peers, TimeToLive) ->
    {'SHUFFLE', {Peers, TimeToLive, self()}}.

-spec message_shufflereply([ppg_peer:peer()]) -> {'SHUFFLEREPLY', [ppg_peer:peer()]}.
message_shufflereply(Peers) ->
    {'SHUFFLEREPLY', Peers}.

-spec message_connectivity(up|kick|down) -> {'CONNECTIVITY', up|kick|down}.
message_connectivity(Direction) ->
    {'CONNECTIVITY', Direction}.
