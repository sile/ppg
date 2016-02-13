%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(ppg_hyparview).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([guess_options/1]).
-export([new/0, new/1]).
-export([is_view/1]).

-export([join/2]).
-export([get_peers/1]).
-export([handle_info/2]).

-export_type([view/0]).
-export_type([options/0, option/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-ifdef(DEBUG).
-define(TAG_JOIN, 16#00).
-define(TAG_FORWARD_JOIN, 16#01).
-define(TAG_CONNECT, 16#02).
-define(TAG_DISCONNECT, 16#03).
-define(TAG_NEIGHBOR, 16#04).
-define(TAG_FOREIGNER, 16#05).
-define(TAG_SHUFFLE, 16#06).
-define(TAG_SHUFFLEREPLY, 16#07).
-else.
-define(TAG_JOIN, 'JOIN').
-define(TAG_FORWARD_JOIN, 'FORWARD_JOIN').
-define(TAG_CONNECT, 'CONNECT').
-define(TAG_DISCONNECT, 'DISCONNECT').
-define(TAG_NEIGHBOR, 'NEIGHBOR').
-define(TAG_FOREIGNER, 'FOREIGNER').
-define(TAG_SHUFFLE, 'SHUFFLE').
-define(TAG_SHUFFLEREPLY, 'SHUFFLEREPLY').
-endif.

-define(VIEW, ?MODULE).

-record(?VIEW,
        {
          %% Dynamic Fields
          active_view = #{}          :: #{ppg_peer:peer() => Monitor::reference()},
          passive_view = #{}         :: #{ppg_peer:peer() => Monitor::reference()}, % TODO: note (dummy or monitor_if_promoting)
          shuffle_timer = make_ref() :: reference(),

          %% Static Fields
          active_view_size           :: pos_integer(),
          passive_view_size          :: pos_integer(),
          active_random_walk_length  :: pos_integer(),
          passive_random_walk_length :: pos_integer(),
          shuffle_count              :: pos_integer(),
          shuffle_interval           :: timeout(),

          %% XXX
          contact_peer :: pid() | undefined
        }).

-opaque view() :: #?VIEW{}.

-type options() :: [option()].

-type option() :: {active_view_size, pos_integer()}
                | {passive_view_size, pos_integer()}
                | {active_random_walk_length, pos_integer()}
                | {passive_random_walk_length, pos_integer()}
                | {shuffle_count, pos_integer()}
                | {shuffle_interval, timeout()}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec guess_options(pos_integer()) -> options().
guess_options(GroupSizeHint) ->
    _ = is_integer(GroupSizeHint) andalso GroupSizeHint > 0 orelse error(badarg, [GroupSizeHint]),

    ActiveViewSize = max(2, round(math:log10(GroupSizeHint) + 1)),
    PassiveViewSize = ActiveViewSize * 6,
    ActiveRandomWalkLength = max(2, round(math:log(GroupSizeHint) / math:log(ActiveViewSize) + 2)),
    PassiveRandomWalkLength = ActiveRandomWalkLength div 2,
    ShuffleCount = max(3, PassiveViewSize div 5),
    ShuffleInterval = 5 * 60 * 1000,
    [
     {active_view_size, ActiveViewSize},
     {passive_view_size, PassiveViewSize},
     {active_random_walk_length, ActiveRandomWalkLength},
     {passive_random_walk_length, PassiveRandomWalkLength},
     {shuffle_count, ShuffleCount},
     {shuffle_interval, ShuffleInterval}
    ].

%% @equiv new(guess_options(100))
-spec new() -> view().
new() ->
    new(guess_options(100)).

-spec new(options()) -> view().
new(Options0) ->
    _ = is_list(Options0) orelse error(badarg, [Options0]),
    Options1 = Options0 ++ guess_options(100),

    Get =
        fun (Key, Validate) ->
                Value = proplists:get_value(Key, Options1),
                _ = Validate(Value) orelse error(badarg, [Options0]),
                Value
        end,
    View0 =
        #?VIEW{
            active_view_size = Get(active_view_size, fun ppg_util:is_pos_integer/1),
            passive_view_size = Get(passive_view_size, fun ppg_util:is_pos_integer/1),
            active_random_walk_length = Get(active_random_walk_length, fun ppg_util:is_pos_integer/1),
            passive_random_walk_length = Get(passive_random_walk_length, fun ppg_util:is_pos_integer/1),
            shuffle_count = Get(shuffle_count, fun ppg_util:is_pos_integer/1),
            shuffle_interval = Get(shuffle_interval, fun ppg_util:is_timeout/1)
           },
    View1 = schedule_shuffle(View0),
    View1.

-spec is_view(view() | term()) -> boolean().
is_view(X) -> is_record(X, ?VIEW).

-spec get_peers(view()) -> [ppg_peer:peer()].
get_peers(#?VIEW{active_view = View}) -> maps:keys(View).

-spec join(ppg_peer:contact_peer(), view()) -> view().
join(ContactPeer, View) ->
    _ = ContactPeer ! message_join(),
    View#?VIEW{contact_peer = ContactPeer}.

-spec handle_info(term(), view()) -> {ok, view()} | ignore.
handle_info({?TAG_JOIN,         Arg}, View) -> {ok, handle_join(Arg, View)};
handle_info({?TAG_FORWARD_JOIN, Arg}, View) -> {ok, handle_forward_join(Arg, View)};
handle_info({?TAG_CONNECT,      Arg}, View) -> {ok, handle_connect(Arg, View)};
handle_info({?TAG_DISCONNECT,   Arg}, View) -> {ok, handle_disconnect(Arg, false, View)};
handle_info({?TAG_NEIGHBOR,     Arg}, View) -> {ok, handle_neighbor(Arg, View)};
handle_info({?TAG_FOREIGNER,    Arg}, View) -> {ok, handle_foreigner(Arg, View)};
handle_info({?TAG_SHUFFLE,      Arg}, View) -> {ok, handle_shuffle(Arg, View)};
handle_info({?TAG_SHUFFLEREPLY, Arg}, View) -> {ok, handle_shufflereply(Arg, View)};
handle_info({'DOWN', Ref, _, Pid, _}, View) ->
    case View of
        #?VIEW{active_view  = #{Pid := Ref}} -> {ok, handle_disconnect(Pid, true, View)};
        #?VIEW{passive_view = #{Pid := Ref}} -> {ok, handle_foreigner(Pid, View)};
        _                                    -> ignore
    end;
handle_info(start_shuffle, View) -> {ok, handle_start_shuffle(View)};
handle_info(_Info, _View) -> ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec schedule_shuffle(view()) -> view().
schedule_shuffle(View = #?VIEW{shuffle_interval = infinity}) ->
    View;
schedule_shuffle(View = #?VIEW{shuffle_interval = Interval}) ->
    _ = erlang:cancel_timer(View#?VIEW.shuffle_timer),
    _ = receive start_shuffle -> ok after 0 -> ok end,

    After = (Interval div 2) + (rand:uniform(Interval + 1) - 1),
    Ref = erlang:send_after(After, self(), start_shuffle),
    View#?VIEW{shuffle_timer = Ref}.

-spec handle_start_shuffle(view()) -> view().
handle_start_shuffle(View = #?VIEW{active_view = Active}) when Active =:= #{} ->
    schedule_shuffle(View);
handle_start_shuffle(View = #?VIEW{shuffle_count = Count}) ->
    ActiveCount = (Count + 1) div 2,
    Peers =
        [self()] ++
        ppg_util:random_select_keys(ActiveCount - 1, View#?VIEW.active_view) ++
        ppg_util:random_select_keys(Count - ActiveCount, View#?VIEW.passive_view),
    Next = ppg_util:random_select_key(View#?VIEW.active_view),
    _ = Next ! message_shuffle(Peers, View#?VIEW.active_random_walk_length),
    schedule_shuffle(View).

-spec handle_join(ppg_peer:peer(), view()) -> view().
handle_join(NewPeer, View0) ->
    View1 = add_peer_to_active_view(NewPeer, true, View0),
    _ = maps:fold(
          fun (P, _, _) ->
                  P =/= NewPeer andalso (P ! message_forward_join(NewPeer, View1#?VIEW.active_random_walk_length))
          end,
          ok,
          View1#?VIEW.active_view),
    View1.

-spec handle_forward_join({ppg_peer:peer(), pos_integer(), ppg_peer:peer()}, view()) -> view().
handle_forward_join({NewPeer, 0, _}, View) ->
    add_peer_to_active_view(NewPeer, true, View);
handle_forward_join({NewPeer, _, _}, View = #?VIEW{active_view = Active}) when Active =:= #{} ->
    add_peer_to_active_view(NewPeer, true, View);
handle_forward_join({NewPeer, TimeToLive, Sender}, View0) ->
    View1 =
        case TimeToLive =:= View0#?VIEW.passive_random_walk_length of
            false -> View0;
            true  -> add_peer_to_passive_view(NewPeer, View0)
        end,
    Next =
        case maps:size(View0#?VIEW.active_view) of
            1 -> Sender;
            _ -> ppg_util:random_select_key(maps:remove(Sender, View0#?VIEW.active_view))
        end,
    _ = Next ! message_forward_join(NewPeer, TimeToLive - 1),
    View1.

-spec handle_connect(ppg_peer:peer(), view()) -> view().
handle_connect(Peer, View) ->
    add_peer_to_active_view(Peer, false, View).

-spec handle_disconnect(ppg_peer:peer(), boolean(), view()) -> view().
handle_disconnect(Peer, IsPeerDown, View0) ->
    View1 = disconnect_peer(Peer, false, View0),
    View2 = promote_passive_peer(View1),
    case IsPeerDown of
        true  -> View2;
        false -> add_peer_to_passive_view(Peer, View2)
    end.

-spec handle_neighbor({high|low, ppg_peer:peer()}, view()) -> view().
handle_neighbor({high, Peer}, View) ->
    add_peer_to_active_view(Peer, true, View);
handle_neighbor({low, Peer}, View) ->
    case maps:size(View#?VIEW.active_view) < View#?VIEW.active_view_size of
        true  -> add_peer_to_active_view(Peer, true, View);
        false -> _ = Peer ! message_foreigner(), View
    end.

-spec handle_foreigner(ppg_peer:peer(), view()) -> view().
handle_foreigner(Peer, View0) ->
    View1 = remove_passive_peer(Peer, View0),
    case maps:size(View1#?VIEW.active_view) < View1#?VIEW.active_view_size of
        false -> View1;
        true  -> promote_passive_peer(View1)
    end.

-spec handle_shuffle({[ppg_peer:peer()], pos_integer(), ppg_peer:peer()}, view()) -> view().
handle_shuffle({Peers, TimeToLive, Sender}, View) ->
    NextCandidates = maps:remove(hd(Peers), maps:remove(Sender, View#?VIEW.active_view)),
    case TimeToLive > 0 andalso maps:size(NextCandidates) > 0 of
        true ->
            Next = ppg_util:random_select_key(NextCandidates),
            _ = Next ! message_shuffle(Peers, TimeToLive - 1),
            View;
        false ->
            ReplyPeers = ppg_util:random_select_keys(length(Peers), View#?VIEW.passive_view),
            _ = hd(Peers) ! message_shufflereply(ReplyPeers),
            add_peers_to_passive_view(Peers, View)
    end.

-spec handle_shufflereply([ppg_peer:peer()], view()) -> view().
handle_shufflereply(Peers, View) ->
    add_peers_to_passive_view(Peers, View).

-spec add_peer_to_active_view(ppg_peer:peer(), boolean(), view()) -> view().
add_peer_to_active_view(Peer, IsActiveConnect, View0) ->
    case Peer =:= self() orelse maps:is_key(Peer, View0#?VIEW.active_view) of
        true  -> View0;
        false ->
            View1 = ensure_active_view_free_space(View0),
            View2 = connect_to_peer(Peer, IsActiveConnect, View1),
            View2
    end.

-spec add_peer_to_passive_view(ppg_peer:peer(), view()) -> view().
add_peer_to_passive_view(Peer, View) ->
    add_peers_to_passive_view([Peer], View).

-spec add_peers_to_passive_view([ppg_peer:peer()], view()) -> view().
add_peers_to_passive_view(Peers0, View0) ->
    Peers1 = Peers0 -- [self() | maps:keys(View0#?VIEW.active_view)],
    View1 = View0#?VIEW{passive_view = maps:without(Peers1, View0#?VIEW.passive_view)},
    View2 = ensure_passive_view_free_space(length(Peers1), View1),
    PassiveView = lists:foldl(fun (P, Acc) -> maps:put(P, make_ref(), Acc) end, View2#?VIEW.passive_view, Peers1),
    View2#?VIEW{passive_view = PassiveView}.

-spec ensure_active_view_free_space(view()) -> view().
ensure_active_view_free_space(View0) ->
    case maps:size(View0#?VIEW.active_view) < View0#?VIEW.active_view_size of
        true  -> View0;
        false ->
            Peer = ppg_util:random_select_key(View0#?VIEW.active_view),
            View1 = disconnect_peer(Peer, true, View0),
            View2 = add_peer_to_passive_view(Peer, View1),
            ensure_active_view_free_space(View2)
    end.

-spec disconnect_peer(ppg_peer:peer(), boolean(), view()) -> view().
disconnect_peer(Peer, IsActiveDisconnect, View) ->
    Monitor = maps:get(Peer, View#?VIEW.active_view, make_ref()), % NOTE: maybe already disconnected
    _ = demonitor(Monitor, [flush]),
    _ = IsActiveDisconnect andalso (Peer ! message_disconnect()),
    ok = ppg_plumtree:notify_neighbor_down(Peer),
    ActiveView = maps:remove(Peer, View#?VIEW.active_view),
    View#?VIEW{active_view = ActiveView}.

-spec ensure_passive_view_free_space(pos_integer(), view()) -> view().
ensure_passive_view_free_space(Room, View) ->
    case maps:size(View#?VIEW.passive_view) =< View#?VIEW.passive_view_size - Room of
        true  -> View;
        false ->
            {{_, Monitor}, PassiveView} = ppg_util:random_pop(View#?VIEW.passive_view),
            _ = demonitor(Monitor, [flush]),
            ensure_passive_view_free_space(1, View#?VIEW{passive_view = PassiveView})
    end.

-spec connect_to_peer(ppg_peer:peer(), boolean(), view()) -> view().
connect_to_peer(Peer, IsActiveConnect, View) ->
    _ = IsActiveConnect andalso (Peer ! message_connect()),
    ok = ppg_plumtree:notify_neighbor_up(Peer),
    Monitor = monitor(process, Peer),
    ActiveView = maps:put(Peer, Monitor, View#?VIEW.active_view),
    remove_passive_peer(Peer, View#?VIEW{active_view = ActiveView}).

-spec promote_passive_peer(view()) -> view().
promote_passive_peer(View = #?VIEW{passive_view = Passive}) when Passive =:= #{} ->
    ContactPeer =View#?VIEW.contact_peer, % TODO: Add handling of a case in which there is no concat peer
    case View#?VIEW.active_view =:= #{} andalso ContactPeer =/= self() of
        false -> View;
        true  -> join(ContactPeer, View)
    end;
promote_passive_peer(View) ->
    Peer = ppg_util:random_select_key(View#?VIEW.passive_view),
    _ = Peer ! message_neighbor(View),
    PassiveView = maps:put(Peer, monitor(process, Peer), View#?VIEW.passive_view),
    View#?VIEW{passive_view = PassiveView}.

-spec remove_passive_peer(ppg_peer:peer(), view()) -> view().
remove_passive_peer(Peer, View = #?VIEW{passive_view = PassiveView}) ->
    _ = demonitor(maps:get(Peer, PassiveView, make_ref()), [flush]),
    View#?VIEW{passive_view = maps:remove(Peer, PassiveView)}.

-spec message_join() -> {?TAG_JOIN, NewPeer::ppg_peer:peer()}.
message_join() -> {?TAG_JOIN, self()}.

-spec message_forward_join(NewPeer, TimeToLive) -> {?TAG_FORWARD_JOIN, {NewPeer, TimeToLive, Sender}} when
      NewPeer    :: ppg_peer:peer(),
      TimeToLive :: pos_integer(),
      Sender     :: ppg_peer:peer().
message_forward_join(NewPeer, TimeToLive) -> {?TAG_FORWARD_JOIN, {NewPeer, TimeToLive, self()}}.

-spec message_connect() -> {?TAG_CONNECT, ppg_peer:peer()}.
message_connect() -> {?TAG_CONNECT, self()}.

-spec message_disconnect() -> {?TAG_DISCONNECT, ppg_peer:peer()}.
message_disconnect() -> {?TAG_DISCONNECT, self()}.

-spec message_neighbor(view()) -> {?TAG_NEIGHBOR, {high|low, ppg_peer:peer()}}.
message_neighbor(#?VIEW{active_view = ActiveView}) ->
    Priority =
        case maps:size(ActiveView) of
            0 -> high;
            _ -> low
        end,
    {?TAG_NEIGHBOR, {Priority, self()}}.

-spec message_foreigner() -> {?TAG_FOREIGNER, ppg_peer:peer()}.
message_foreigner() -> {?TAG_FOREIGNER, self()}.

-spec message_shuffle(Peers, pos_integer()) -> {?TAG_SHUFFLE, {Peers, pos_integer(), ppg_peer:peer()}} when
      Peers :: [ppg_peer:peer()].
message_shuffle(Peers, TimeToLive) -> {?TAG_SHUFFLE, {Peers, TimeToLive, self()}}.

-spec message_shufflereply([ppg_peer:peer()]) -> {?TAG_SHUFFLEREPLY, [ppg_peer:peer()]}.
message_shufflereply(Peers) -> {?TAG_SHUFFLEREPLY, Peers}.
