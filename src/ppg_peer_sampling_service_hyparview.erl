%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc A HyParView implemntation of the ppg_peer_sampling_service interface
%%
%% TODO: shuffle
-module(ppg_peer_sampling_service_hyparview).

-behaviour(ppg_peer_sampling_service).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/0, new/1]).

-export_type([option/0, options/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'ppg_peer_sampling_service' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([join/2, leave/1, get_peers/1, handle_info/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(TAG, hyparview). % NOTE: "このタグはノード間通信されるメッセージに含まれるのでサイズが小さいほうが良い"的なコメントを書く and 最終的には小さな整数値にしてしまう

-define(STATE, ?MODULE).

-record(opt,
        {
          active_view_size           = auto :: pos_integer() | auto,
          passive_view_size          = auto :: pos_integer() | auto,
          active_random_walk_length  = auto :: pos_integer() | auto,
          passive_random_walk_length = auto :: pos_integer() | auto
        }).

-record(?STATE,
        {
          active_view                = [] :: [pid()],
          passive_view               = [] :: [pid()],
          monitors                   = #{}:: #{reference() => pid()},
          active_view_size           = 5  :: pos_integer(),
          passive_view_size          = 30 :: pos_integer(),
          active_random_walk_length  = 6  :: pos_integer(),
          passive_random_walk_length = 3  :: pos_integer(),
          opt                             :: #opt{}
        }).

-type options() :: [option()].

-type option() :: {group_size_hint, pos_integer()} % default and auto: length(nodes() + 1)
                | {active_view_size, pos_integer() | auto} % 5 (auto = log10(n) + 1)
                | {passive_view_size, pos_integer() | auto} % 30 (auto = active_view_size * 6)
                | {active_random_walk_length, pos_integer() | auto} % 6 (auto = depth(plumtree-panning-tree = diameter))
                | {passive_random_walk_length, pos_integer() | auto}. % 3 (auto = ceil(arwl / 2))

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @equiv new([])
-spec new() -> ppg_peer_sampling_service:instance().
new() ->
    new([]).

-spec new(options()) -> ppg_peer_sampling_service:instance().
new(Options) ->
    _ = is_list(Options) orelse error(badarg, [Options]),

    ValidatePosIntOrAuto =
        fun (Key) ->
                Value = proplists:get_value(Key, Options, auto),
                (is_integer(Value) andalso Value > 0) orelse Value =:= auto orelse error(badarg, [Options]),
                Value
        end,
    GroupSizeHint =
        case ValidatePosIntOrAuto(group_size_hint) of
            auto -> length(nodes()) + 1;
            Hint -> Hint
        end,
    Opt =
        #opt{
           active_view_size           = ValidatePosIntOrAuto(active_view_size),
           passive_view_size          = ValidatePosIntOrAuto(passive_view_size),
           active_random_walk_length  = ValidatePosIntOrAuto(active_random_walk_length),
           passive_random_walk_length = ValidatePosIntOrAuto(passive_random_walk_length)
          },
    State0 = #?STATE{opt = Opt},
    State1 = calculate_coefficients(GroupSizeHint, round(math:log2(GroupSizeHint)) + 1, State0),

    ppg_peer_sampling_service:new(?MODULE, State1).

%%----------------------------------------------------------------------------------------------------------------------
%% 'ppg_peer_sampling_service' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
join(ContactPeer, State) ->
    _ = ContactPeer ! message_join(),
    State.

%% @private
leave(State) ->
    State.

%% @private
get_peers(State0) ->
    Peers = [],
    State1 = State0,
    {Peers, State1}.

%% @private
handle_info({?TAG, 'JOIN', Arg}, State) ->
    handle_join(Arg, State);
handle_info({?TAG, 'FORWARD_JOIN', Arg}, State) ->
    handle_forward_join(Arg, State);
handle_info({?TAG, 'CONNECT', Arg}, State) ->
    handle_connect(Arg, State);
handle_info({?TAG, 'DISCONNECT', Arg}, State) ->
    handle_disconnect(Arg, State);
handle_info({?TAG, 'NEIGHBOR', Arg}, State) ->
    handle_neighbor(Arg, State);
handle_info({'DOWN', Ref, _, Peer, _}, State) ->
    case maps:is_key(Ref, State#?STATE.monitors) of
        false -> ignore;
        true  -> handle_down(Peer, State)
    end;
handle_info(_, _) ->
    ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec calculate_coefficients(pos_integer(), pos_integer(), #?STATE{}) -> #?STATE{}.
calculate_coefficients(GroupSize, TreeDepth, State = #?STATE{opt = Opt}) ->
    case Opt#opt.active_view_size of
        auto           -> ActiveViewSize = max(3, round(math:log10(GroupSize)) + 1);
        ActiveViewSize -> ActiveViewSize
    end,
    case Opt#opt.passive_view_size of
        auto            -> PassiveViewSize = ActiveViewSize * 6;
        PassiveViewSize -> PassiveViewSize
    end,
    case Opt#opt.active_random_walk_length of
        auto -> ARWL = max(3, TreeDepth);
        ARWL -> ARWL
    end,
    case Opt#opt.passive_random_walk_length of
        auto  -> PRWL = ARWL div 2;
        PRWL0 -> PRWL = min(PRWL0, ARWL)
    end,
    State#?STATE{
             active_view_size           = ActiveViewSize,
             passive_view_size          = PassiveViewSize,
             active_random_walk_length  = ARWL,
             passive_random_walk_length = PRWL
            }.

-spec handle_join(pid(), #?STATE{}) -> {ok, #?STATE{}}.
handle_join(Peer, State0) ->
    State1 = add_peer_to_active_view(Peer, true, State0),
    ok = lists:foreach(
           fun (P) -> P =/= Peer andalso (P ! message_forward_join(Peer, State1)) end,
           State1#?STATE.active_view),
    {ok, State1}.

-spec handle_forward_join({pid(), pos_integer(), pid()}, #?STATE{}) -> {ok, #?STATE{}}.
handle_forward_join({Peer, 0, _}, State) ->
    {ok, add_peer_to_active_view(Peer, true, State)};
handle_forward_join({Peer, _, _}, State = #?STATE{active_view = []}) ->
    {ok, add_peer_to_active_view(Peer, true, State)};
handle_forward_join({Peer, TimeToLive, Sender}, State0) ->
    State1 =
        case TimeToLive =:= State0#?STATE.passive_random_walk_length of
            false -> State0;
            true  -> add_peer_to_passive_view(Peer, State0)
        end,
    Next =
        case lists:delete(Sender, State1#?STATE.active_view) of
            []         -> Sender;
            Candidates -> ppg_util:select_random(Candidates) % TODO: consider channels weight
        end,
    _ = Next ! message_forward_join(Peer, TimeToLive - 1, State1),
    {ok, State1}.

-spec handle_connect(pid(), #?STATE{}) -> {ok, #?STATE{}}.
handle_connect(Peer, State) ->
    {ok, add_peer_to_active_view(Peer, false, State)}.

-spec handle_disconnect(pid(), #?STATE{}) -> {ok, #?STATE{}}.
handle_disconnect(Peer, State0) ->
    ActiveView = lists:delete(Peer, State0#?STATE.active_view),
    Monitors =
        maps:filter(fun (Ref, P) when P =:= Peer ->
                            _ = demonitor(Ref, [flush]),
                            ok = ppg_peer_sampling_service:notify_neighbor_down(Peer),
                            false;
                        (_, _) ->
                            true
                    end,
                    State0#?STATE.monitors),
    State1 = promote_passive_peer(State0),
    State2 = add_peer_to_passive_view(Peer, State1#?STATE{active_view = ActiveView, monitors = Monitors}),
    {ok, State2}.

-spec handle_down(pid(), #?STATE{}) -> {ok, #?STATE{}}.
handle_down(Peer, State0) ->
    ActiveView = lists:delete(Peer, State0#?STATE.active_view),
    Monitors =
        maps:filter(fun (Ref, P) when P =:= Peer ->
                            _ = demonitor(Ref, [flush]),
                            ok = ppg_peer_sampling_service:notify_neighbor_down(Peer),
                            false;
                        (_, _) ->
                            true
                    end,
                    State0#?STATE.monitors),
    PassiveView = lists:delete(Peer, State0#?STATE.passive_view),
    State1 = State0#?STATE{active_view = ActiveView, passive_view = PassiveView, monitors = Monitors},
    State2 = promote_passive_peer(State1),
    {ok, State2}.

-spec handle_neighbor({high|low, pid()}, #?STATE{}) -> {ok, #?STATE{}}.
handle_neighbor({Priority, Peer}, State0) ->
    case lists:member(Peer, State0#?STATE.active_view) of
        true  -> {ok, State0};
        false ->
            case Priority =:= high orelse length(State0#?STATE.active_view) < State0#?STATE.active_view_size of
                false ->
                    _ = Peer ! message_disconnect(),
                    {ok, State0};
                true ->
                    State1 = add_peer_to_active_view(Peer, true, State0),
                    {ok, State1}
            end
    end.

-spec promote_passive_peer(#?STATE{}) -> #?STATE{}.
promote_passive_peer(State) ->
    Candidates = State#?STATE.passive_view -- State#?STATE.active_view,
    _ = Candidates =/= [] andalso
        begin
            Peer = ppg_util:select_random(Candidates),
            Peer ! message_neighbor(State)
        end,
    State.
    %% case Candidates of
    %%     [] -> State;
    %%     _  ->
    %%         {Peer, PassiveView} = ppg_util:delete_random(Candidates),
    %%         _ = Peer ! message_neighbor(State),
    %%         State#?STATE{passive_view = PassiveView}
    %% end.

-spec add_peer_to_active_view(pid(), boolean(), #?STATE{}) -> #?STATE{}.
add_peer_to_active_view(Peer, SendConnect, State0) ->
    %% NOTE: The `Peer' may exist in the passive_view (TODO: should the peer is deleted from the passive_view ?)
    IsNew = Peer =/= self() andalso not lists:member(Peer, State0#?STATE.active_view),
    case IsNew of
        false -> State0;
        true  ->
            State1 = ensure_active_view_free_space(State0),
            _ = SendConnect andalso (Peer ! message_connect()),
            ActiveView = [Peer | State1#?STATE.active_view],
            ok = ppg_peer_sampling_service:notify_neighbor_up(Peer),
            Monitors = maps:put(monitor(process, Peer), Peer, State1#?STATE.monitors),
            State1#?STATE{active_view = ActiveView, monitors = Monitors}
    end.

-spec add_peer_to_passive_view(pid(), #?STATE{}) -> #?STATE{}.
add_peer_to_passive_view(Peer, State0) ->
    IsNew =
        Peer =/= self() andalso
        not lists:member(Peer, State0#?STATE.active_view) andalso
        not lists:member(Peer, State0#?STATE.passive_view),
    case IsNew of
        false -> State0;
        true  ->
            State1 = ensure_passive_view_free_space(State0),
            PassiveView = [Peer | State1#?STATE.passive_view],
            State1#?STATE{passive_view = PassiveView}
    end.

-spec ensure_active_view_free_space(#?STATE{}) -> #?STATE{}.
ensure_active_view_free_space(State = #?STATE{active_view = View, active_view_size = Size}) when length(View) < Size ->
    State;
ensure_active_view_free_space(State0) ->
    {DeletedPeer, ActiveView} = ppg_util:delete_random(State0#?STATE.active_view),
    _ = DeletedPeer ! message_disconnect(),
    ok = ppg_peer_sampling_service:notify_neighbor_down(DeletedPeer),
    Monitors =
        maps:filter(fun (Ref, P) when P =:= DeletedPeer ->
                            _ = demonitor(Ref, [flush]),
                            false;
                        (_, _) ->
                            true
                    end,
                    State0#?STATE.monitors),
    State1 = add_peer_to_passive_view(DeletedPeer, State0#?STATE{active_view = ActiveView, monitors = Monitors}),
    ensure_active_view_free_space(State1).

-spec ensure_passive_view_free_space(#?STATE{}) -> #?STATE{}.
ensure_passive_view_free_space(State = #?STATE{passive_view = View, passive_view_size = Size}) when length(View) < Size ->
    State;
ensure_passive_view_free_space(State0) ->
    {_, PassiveView} = ppg_util:delete_random(State0#?STATE.passive_view),
    State1 = State0#?STATE{passive_view = PassiveView},
    ensure_passive_view_free_space(State1).

-spec message_join() -> {?TAG, 'JOIN', pid()}.
message_join() ->
    {?TAG, 'JOIN', self()}.

-spec message_forward_join(pid(), #?STATE{}) -> {?TAG, 'FORWARD_JOIN', {pid(), pos_integer(), pid()}}.
message_forward_join(Peer, State) ->
    {?TAG, 'FORWARD_JOIN', {Peer, State#?STATE.active_random_walk_length, self()}}.

-spec message_forward_join(pid(), non_neg_integer(), #?STATE{}) -> {?TAG, 'FORWARD_JOIN', {pid(), pos_integer(), pid()}}.
message_forward_join(Peer, TimeToLive, _State) ->
    {?TAG, 'FORWARD_JOIN', {Peer, TimeToLive, self()}}. % XXX:

-spec message_connect() -> {?TAG, 'CONNECT', pid()}.
message_connect() ->
    {?TAG, 'CONNECT', self()}.

-spec message_disconnect() -> {?TAG, 'DISCONNECT', pid()}.
message_disconnect() ->
    {?TAG, 'DISCONNECT', self()}.

-spec message_neighbor(#?STATE{}) -> {?TAG, 'NEIGHBOR', {high|low, pid()}}.
message_neighbor(#?STATE{active_view = []}) ->
    {?TAG, 'NEIGHBOR', {high, self()}};
message_neighbor(_) ->
    {?TAG, 'NEIGHBOR', {low, self()}}.
