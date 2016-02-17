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

-export([new/1, new/2]).
-export([join/1]).
-export([get_peers/1]).
-export([flush_queue/1]).
-export([handle_info/2]).

-export_type([view/0]).
-export_type([connection/0]).
-export_type([options/0, option/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(TAG_JOIN,         'JOIN').
-define(TAG_FORWARD_JOIN, 'FORWARD_JOIN').
-define(TAG_CONNECT,      'CONNECT').
-define(TAG_DISCONNECT,   'DISCONNECT').
-define(TAG_NEIGHBOR,     'NEIGHBOR').
-define(TAG_FOREIGNER,    'FOREIGNER').
-define(TAG_SHUFFLE,      'SHUFFLE').
-define(TAG_SHUFFLEREPLY, 'SHUFFLEREPLY').
-define(TAG_CONNECTIVITY, 'CONNECTIVITY').

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

          %% Internal Event Queue
          queue = [] :: [event()],

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
-opaque connection() :: reference().

-type options() :: [option()].

-type option() :: {active_view_size, pos_integer()}
                | {passive_view_size, pos_integer()}
                | {active_random_walk_length, pos_integer()}
                | {passive_random_walk_length, pos_integer()}
                | {shuffle_count, pos_integer()}
                | {shuffle_interval, timeout()}
                | {max_broadcast_delay, timeout()}
                | {allowable_disconnection_period, timeout()}.

-type ttl() :: non_neg_integer().
%% Time To Live

-type event() :: {up, ppg_peer:peer(), connection()}
               | {down, ppg_peer:peer(), connection()}
               | {broadcast, term()}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% NOTE: 1000ノードくらいまでならこのパラメータで十分
-spec default_options() -> options().
default_options() ->
    [
     {active_view_size, 4},
     {passive_view_size, 24},
     {active_random_walk_length, 6},
     {passive_random_walk_length, 3},
     {shuffle_count, 4},
     {shuffle_interval, 10 * 60 * 1000},
     {max_broadcast_delay, 10 * 1000},
     {allowable_disconnection_period, 60 * 1000}
    ].

%% @equiv new(Group, default_options())
-spec new(ppg:name()) -> view().
new(Group) ->
    new(Group, default_options()).

-spec new(ppg:name(), options()) -> view().
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

-spec get_peers(view()) -> [{ppg_peer:peer(), connection()}].
get_peers(View) ->
    maps:to_list(View#?VIEW.active_view).

-spec join(view()) -> view().
join(View) ->
    ContactPeer = ppg_contact_service:get_peer(View#?VIEW.contact_service),
    case ContactPeer =:= self() of
        true  -> View;
        false ->
            _ = ContactPeer ! message_join(),
            %% Joined peer must be connected with at least one other peer (or timed out in the caller side)
            receive
                {?TAG_CONNECT, Arg} -> handle_connect(Arg, View)
            end
    end.

-spec flush_queue(view()) -> {[event()], view()}.
flush_queue(View) ->
    {lists:reverse(View#?VIEW.queue), View#?VIEW{queue = []}}.

-spec handle_info(term(), view()) -> {ok, view()} | ignore.
handle_info({?TAG_JOIN,         Arg}, View) -> {ok, handle_join(Arg, View)};
handle_info({?TAG_FORWARD_JOIN, Arg}, View) -> {ok, handle_forward_join(Arg, View)};
handle_info({?TAG_CONNECT,      Arg}, View) -> {ok, handle_connect(Arg, View)};
handle_info({?TAG_DISCONNECT,   Arg}, View) -> {ok, handle_disconnect(Arg, false, View)};
handle_info({?TAG_NEIGHBOR,     Arg}, View) -> {ok, handle_neighbor(Arg, View)};
handle_info({?TAG_FOREIGNER,    Arg}, View) -> {ok, handle_foreigner(Arg, View)};
handle_info({?TAG_SHUFFLE,      Arg}, View) -> {ok, handle_shuffle(Arg, View)};
handle_info({?TAG_SHUFFLEREPLY, Arg}, View) -> {ok, handle_shufflereply(Arg, View)};
handle_info({?TAG_CONNECTIVITY, Arg}, View) -> {ok, handle_connectivity(Arg, View)};
handle_info({?MODULE, start_shuffle}, View) -> {ok, handle_start_shuffle(View)};
handle_info({?MODULE, rejoin},        View) -> {ok, join(View)};
handle_info({'DOWN', Ref, _, Pid, _}, View) ->
    case View of
        #?VIEW{monitors = #{Pid := Ref}} ->
            case View of
                #?VIEW{active_view = #{Pid := Conn}} -> {ok, handle_disconnect({Conn, Pid}, true, View)};
                #?VIEW{passive_view = #{Pid := _}}   -> {ok, handle_foreigner(Pid, View)}
            end;
        _ -> ignore
    end;
handle_info(_Info, _View) -> ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_join(ppg_peer:peer(), view()) -> view().
handle_join(NewPeer, View0 = #?VIEW{active_random_walk_length = ARWL}) ->
    View1 = add_peer_to_active_view(NewPeer, undefined, View0),
    ok = ppg_maps:foreach(
           fun (P, _) -> P =/= NewPeer andalso (P ! message_forward_join(NewPeer, ARWL)) end,
           View1#?VIEW.active_view),
    View1.

-spec handle_forward_join({ppg_peer:peer(), ttl(), ppg_peer:peer()}, view()) -> view().
handle_forward_join({NewPeer, 0, _}, View) ->
    add_peer_to_active_view(NewPeer, undefined, View);
handle_forward_join({NewPeer, _, _}, View = #?VIEW{active_view = Active}) when Active =:= #{} ->
    add_peer_to_active_view(NewPeer, undefined, View);
handle_forward_join({NewPeer, TimeToLive, Sender}, View) ->
    Next = ppg_maps:random_key(maps:remove(Sender, View#?VIEW.active_view), Sender),
    _ = Next ! message_forward_join(NewPeer, TimeToLive - 1),
    case TimeToLive =:= View#?VIEW.passive_random_walk_length of
        false -> View;
        true  -> add_peers_to_passive_view([NewPeer], View)
    end.

-spec handle_connect({connection(), ppg_peer:peer()}, view()) -> view().
handle_connect({Conn, Peer}, View) ->
    add_peer_to_active_view(Peer, Conn, View).

-spec handle_disconnect({connection(), ppg_peer:peer()}, boolean(), view()) -> view().
handle_disconnect({Connection, Peer}, IsPeerDown, View0) ->
    case disconnect_peer(Peer, Connection, View0) of
        error       -> View0;
        {ok, View1} ->
            View2 = promote_passive_peer_if_needed(View1),
            View3 = start_connectivity_check_if_needed(View2),
            case IsPeerDown of
                true  -> View3;
                false -> add_peers_to_passive_view([Peer], View3)
            end
    end.

-spec handle_neighbor({high|low, ppg_peer:peer()}, view()) -> view().
handle_neighbor({high, Peer}, View) -> add_peer_to_active_view(Peer, undefined, View);
handle_neighbor({low,  Peer}, View) ->
    case maps:size(View#?VIEW.active_view) < View#?VIEW.active_view_size of
        true  -> add_peer_to_active_view(Peer, undefined, View);
        false -> _ = Peer ! message_foreigner(), View
    end.

-spec handle_foreigner(ppg_peer:peer(), view()) -> view().
handle_foreigner(Peer, View0) ->
    %% TODO: 論文中ではpassiveを削除しない、と書かれているのでそれに合わせる? => 実装の簡潔性を取りたいので注記付きで今の実装のままにする
    View1 = remove_passive_peer(Peer, View0),
    promote_passive_peer_if_needed(View1).

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
            %% TODO: より厳格な接続性の保証を行う
            _ = ContactPeer ! message_connectivity(up),

            After = View#?VIEW.allowable_disconnection_period,
            Timer = ppg_util:cancel_and_send_after(View#?VIEW.rejoin_timer, After, self(), {?MODULE, rejoin}),
            View#?VIEW{rejoin_timer = Timer}
    end.

-spec schedule_shuffle(view()) -> view().
schedule_shuffle(View = #?VIEW{shuffle_interval = infinity}) ->
    View;
schedule_shuffle(View = #?VIEW{shuffle_interval = Interval}) ->
    After = (Interval div 2) + (rand:uniform(Interval + 1) - 1),
    Timer = ppg_util:cancel_and_send_after(View#?VIEW.shuffle_timer, After, self(), {?MODULE, start_shuffle}),
    View#?VIEW{shuffle_timer = Timer}.

-spec handle_start_shuffle(view()) -> view().
handle_start_shuffle(View = #?VIEW{active_view = Active}) when Active =:= #{} ->
    schedule_shuffle(View);
handle_start_shuffle(View = #?VIEW{shuffle_count = Count}) ->
    ActiveCount = (Count + 1) div 2,
    Peers =
        [self()] ++
        ppg_maps:random_keys(ActiveCount - 1, View#?VIEW.active_view) ++
        ppg_maps:random_keys(Count - ActiveCount, View#?VIEW.passive_view),
    {ok, Next} = ppg_maps:random_key(View#?VIEW.active_view),
    _ = Next ! message_shuffle(Peers, View#?VIEW.active_random_walk_length),
    schedule_shuffle(View).

-spec handle_connectivity(up|kick|down, view()) -> view().
handle_connectivity(up, View) ->
    ContactPeer = ppg_contact_service:get_peer(View#?VIEW.contact_service),
    case ContactPeer =:= self() of
        false -> _ = ContactPeer ! message_connectivity(up), View; % Forwards to the latest contact peer
        true  ->
            case erlang:read_timer(View#?VIEW.connectivity_timer) =/= false of
                true  -> View; % The timer has been set
                false ->
                    After = max(0, View#?VIEW.allowable_disconnection_period - View#?VIEW.max_broadcast_delay),
                    Timer = erlang:send_after(After, self(), message_connectivity(kick)),
                    View#?VIEW{connectivity_timer = Timer}
            end
    end;
handle_connectivity(kick, View) ->
    enqueue_event({broadcast, message_connectivity(down)}, View);
handle_connectivity(down, View) ->
    _ = ppg_util:cancel_and_flush_timer(View#?VIEW.rejoin_timer, {?MODULE, rejoin}),
    View.

-spec add_peer_to_active_view(ppg_peer:peer(), connection()|undefined, view()) -> view().
add_peer_to_active_view(Peer,_Connection, View) when Peer =:= self() ->
    View;
add_peer_to_active_view(Peer, Connection, View0) ->
    case maps:find(Peer, View0#?VIEW.active_view) of
        error ->
            %% A new peer; We add the peer to the active view
            View1 = ensure_active_view_free_space(View0),
            connect_to_peer(Peer, Connection, View1);
        {ok, Established} ->
            %% The peer already exists in the active view
            case Connection =:= undefined orelse Connection =< Established of
                true  -> View0; % `Established' is up to date (ignores `Connection')
                false ->
                    %% `Established' is out of date (implicitly reconnected)
                    View1 = enqueue_event({down, Peer, Established}, View0),
                    View2 = enqueue_event({up, Peer, Connection}, View1),
                    View2#?VIEW{active_view = maps:put(Peer, Connection, View1#?VIEW.active_view)}
            end
    end.

-spec ensure_active_view_free_space(view()) -> view().
ensure_active_view_free_space(View0) ->
    case maps:size(View0#?VIEW.active_view) < View0#?VIEW.active_view_size of
        true  -> View0;
        false ->
            {ok, Peer} = ppg_maps:random_key(View0#?VIEW.active_view),
            {ok, View1} = disconnect_peer(Peer, undefined, View0),
            View2 = add_peers_to_passive_view([Peer], View1),
            ensure_active_view_free_space(View2)
    end.

-spec connect_to_peer(ppg_peer:peer(), connection()|undefined, view()) -> view().
connect_to_peer(Peer, undefined, View) ->
    Connection = make_ref(),
    _ = Peer ! message_connect(Connection),
    connect_to_peer(Peer, Connection, View);
connect_to_peer(Peer, Connection, View0) ->
    View1 = remove_passive_peer(Peer, View0),
    View2 = enqueue_event({up, Peer, Connection}, View1),
    ActiveView = maps:put(Peer, Connection, View2#?VIEW.active_view),
    Monitors = maps:put(Peer, monitor(process, Peer), View2#?VIEW.monitors),
    View2#?VIEW{active_view = ActiveView, monitors = Monitors}.

-spec disconnect_peer(ppg_peer:peer(), connection()|undefined, view()) -> {ok, view()} | error.
disconnect_peer(Peer, undefined, View) ->
    Connection = maps:get(Peer, View#?VIEW.active_view),
    _ = Peer ! message_disconnect(Connection),
    disconnect_peer(Peer, Connection, View);
disconnect_peer(Peer, Connection, View0) ->
    case View0#?VIEW.active_view of
        #{Peer := Connection} ->
            View1 = enqueue_event({down, Peer, Connection}, View0),
            _ = demonitor(maps:get(Peer, View1#?VIEW.monitors), [flush]),
            Monitors = maps:remove(Peer, View1#?VIEW.monitors),
            ActiveView = maps:remove(Peer, View1#?VIEW.active_view),
            {ok, View1#?VIEW{active_view = ActiveView, monitors = Monitors}};
        _ ->
            %% The connection is already disconnected
            error
    end.

-spec remove_passive_peer(ppg_peer:peer(), view()) -> view().
remove_passive_peer(Peer, View) ->
    _ = demonitor(maps:get(Peer, View#?VIEW.monitors, make_ref()), [flush]),
    PassiveView = maps:remove(Peer, View#?VIEW.passive_view),
    View#?VIEW{passive_view = PassiveView}.

-spec enqueue_event(event(), view()) -> view().
enqueue_event(Event, View) ->
    View#?VIEW{queue = [Event | View#?VIEW.queue]}.

-spec add_peers_to_passive_view([ppg_peer:peer()], view()) -> view().
add_peers_to_passive_view(Peers0, View0) ->
    Peers1 = Peers0 -- [self() | maps:keys(View0#?VIEW.active_view)],
    View1 = View0#?VIEW{passive_view = maps:without(Peers1, View0#?VIEW.passive_view)}, % NOTE: 後でまた追加するのでdemonitorはしない
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

-spec promote_passive_peer_if_needed(view()) -> view().
promote_passive_peer_if_needed(View) ->
    ActivePeerCount = maps:size(View#?VIEW.active_view),
    case ActivePeerCount < View#?VIEW.active_view_size of
        false -> View;
        true  ->
            %% NOTE: remove in progress peers
            Candidates = maps:filter(fun (P, _) -> not maps:is_key(P, View#?VIEW.monitors) end, View#?VIEW.passive_view),
            case {ppg_maps:random_key(Candidates), ActivePeerCount} of
                {error,      0} -> join(View);
                {error,      _} -> View;
                {{ok, Peer}, _} ->
                    _ = Peer ! message_neighbor(View),
                    Monitors = maps:put(Peer, monitor(process, Peer), View#?VIEW.monitors),
                    View#?VIEW{monitors = Monitors}
            end
    end.

-spec message_join() -> {?TAG_JOIN, NewPeer::ppg_peer:peer()}.
message_join() ->
    {?TAG_JOIN, self()}.

-spec message_forward_join(NewPeer, TimeToLive) -> {?TAG_FORWARD_JOIN, {NewPeer, TimeToLive, Sender}} when
      NewPeer    :: ppg_peer:peer(),
      TimeToLive :: pos_integer(),
      Sender     :: ppg_peer:peer().
message_forward_join(NewPeer, TimeToLive) ->
    {?TAG_FORWARD_JOIN, {NewPeer, TimeToLive, self()}}.

-spec message_connect(connection()) -> {?TAG_CONNECT, {connection(), ppg_peer:peer()}}.
message_connect(Conn) ->
    {?TAG_CONNECT, {Conn, self()}}.

-spec message_disconnect(connection()) -> {?TAG_DISCONNECT, {connection(), ppg_peer:peer()}}.
message_disconnect(Conn) ->
    {?TAG_DISCONNECT, {Conn, self()}}.

-spec message_neighbor(view()) -> {?TAG_NEIGHBOR, {high|low, ppg_peer:peer()}}.
message_neighbor(#?VIEW{active_view = ActiveView}) ->
    Priority = case maps:size(ActiveView) of 0 -> high; _ -> low end,
    {?TAG_NEIGHBOR, {Priority, self()}}.

-spec message_foreigner() -> {?TAG_FOREIGNER, ppg_peer:peer()}.
message_foreigner() ->
    {?TAG_FOREIGNER, self()}.

-spec message_shuffle(Peers, pos_integer()) -> {?TAG_SHUFFLE, {Peers, pos_integer(), ppg_peer:peer()}} when
      Peers :: [ppg_peer:peer()].
message_shuffle(Peers, TimeToLive) ->
    {?TAG_SHUFFLE, {Peers, TimeToLive, self()}}.

-spec message_shufflereply([ppg_peer:peer()]) -> {?TAG_SHUFFLEREPLY, [ppg_peer:peer()]}.
message_shufflereply(Peers) ->
    {?TAG_SHUFFLEREPLY, Peers}.

-spec message_connectivity(up|kick|down) -> {?TAG_CONNECTIVITY, up|kick|down}.
message_connectivity(Direction) ->
    {?TAG_CONNECTIVITY, Direction}.
