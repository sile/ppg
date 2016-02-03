%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc The pg2 implemntation of ppg_peer_sampling_service interface
%%
%% NOTICE: This module is provided only for debugging purposes.
-module(ppg_peer_sampling_service_pg2).

-behaviour(ppg_peer_sampling_service).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/0, new/1]).

-export_type([pg2_group_name/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'ppg_peer_sampling_service' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([join/2, leave/1, get_peers/1, handle_info/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(STATE, ?MODULE).

-record(?STATE,
        {
          pg2_group :: pg2_group_name(),
          monitors = #{} :: #{reference() => pid()},
          info_tag = make_ref() :: reference()
        }).

-type pg2_group_name() :: term().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @equiv new(ppg_peer_sampling_service_pg2)
-spec new() -> ppg_peer_sampling_service:instance().
new() ->
    new(?MODULE).

-spec new(pg2_group_name()) -> ppg_peer_sampling_service:instance().
new(Pg2Group) ->
    State = #?STATE{pg2_group = Pg2Group},
    ppg_peer_sampling_service:new(?MODULE, State).

%%----------------------------------------------------------------------------------------------------------------------
%% 'ppg_peer_sampling_service' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
join(_ContactPeer, State) ->
    ok = pg2:create(State#?STATE.pg2_group),
    ok = pg2:join(State#?STATE.pg2_group, self()),
    ok = schedule_member_check(State),
    State.

%% @private
leave(State) ->
    ok = pg2:leave(State#?STATE.pg2_group, self()),
    State.

%% @private
get_peers(State0) ->
    Num = 5, % TODO: Can be specified by an argument
    Members = pg2:get_members(State0#?STATE.pg2_group) -- [self()],
    Peers = lists:sublist(shuffle(Members), Num),
    Monitors = maps:from_list([{monitor(process, P), P} || P <- Peers]),
    State1 = State0#?STATE{monitors = Monitors},
    {Peers, State1}.

%% @private
handle_info({Tag, member_check}, State = #?STATE{info_tag = Tag}) ->
    handle_member_check(State);
handle_info({'DOWN', Ref, _, _, _}, State) ->
    case maps:is_key(Ref, State#?STATE.monitors) of
        false -> ignore;
        true  -> handle_down(Ref, State)
    end;
handle_info(_, _) ->
    ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec shuffle(list()) -> list().
shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- List])].

-spec schedule_member_check(#?STATE{}) -> ok.
schedule_member_check(State) ->
    _ = erlang:send_after(1000, self(), {State#?STATE.info_tag, member_check}),
    ok.

-spec handle_member_check(#?STATE{}) -> {ok, #?STATE{}}.
handle_member_check(State0) ->
    Num = 5, % TODO:
    Monitors0 = maps:size(State0#?STATE.monitors),
    Monitors1 =
        case maps:values(Monitors0) -- pg2:get_members(State0#?STATE.pg2_group) of
            []        -> Monitors0;
            LeftPeers ->
                lists:foldl(
                  fun (P, Acc) ->
                          _ = demonitor(P, [flush]),
                          ok = ppg_peer_sampling_service:notify_neighbor_down(P),
                          maps:filter(fun (_, Pid) -> Pid =/= P end, Acc)
                  end,
                  Monitors0,
                  LeftPeers)
        end,
    State1 =
        case maps:size(Monitors1) < Num of
            false -> State0;
            true  ->
                Shortage = Num - maps:size(Monitors1),
                Members = pg2:get_members(State0#?STATE.pg2_group) -- [self() | maps:values(Monitors1)],
                Peers = lists:sublist(shuffle(Members), Shortage),
                Monitors2 =
                    lists:foldl(
                      fun (P, Acc) ->
                              ok = ppg_peer_sampling_service:notify_neighbor_up(P),
                              maps:put(monitor(process, P), P, Acc)
                      end,
                      Monitors1,
                      Peers),
                State0#?STATE{monitors = Monitors2}
        end,
    ok = schedule_member_check(State1),
    State1.

-spec handle_down(reference(), #?STATE{}) -> {ok, #?STATE{}}.
handle_down(Ref, State0) ->
    Peer = maps:get(Ref, State0#?STATE.monitors),
    ok = ppg_peer_sampling_service:notify_neighbor_down(Peer),
    Monitors = maps:remove(Ref, State0#?STATE.monitors),
    State1 = State0#?STATE{monitors = Monitors},
    {ok, State1}.
