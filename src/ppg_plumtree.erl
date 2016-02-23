%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc A Plumtree Implementation
%%
%% @reference See http://homepages.gsd.inesc-id.pt/~jleitao/pdf/srds07-leitao.pdf
%% @private
-module(ppg_plumtree).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([default_options/0]).

-export([new/2]).
-export([broadcast/2]).
-export([system_broadcast/2]).
-export([handle_info/3]).
-export([neighbor_up/3]).
-export([neighbor_down/2]).
-export([get_member/1]).
-export([get_peers/1]).

-export_type([tree/0]).
-export_type([peer/0]).
-export_type([peer_type/0]).
-export_type([message/0]).
-export_type([view/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-record(peer,
        {
          pid                     :: ppg_peer:peer(),
          type = eager            :: peer_type(),
          ihave = gb_sets:empty() :: gb_sets:set(message_id()),
          in_tick = false         :: boolean()
        }).

-record(schedule,
        {
          timer = make_ref()   :: reference(),
          queue = ppg_pq:new() :: ppg_pq:heap({ppg_util:milliseconds(), event()})
        }).

-define(TREE, ?MODULE).
-record(?TREE,
        {
          %% Group Member
          member :: ppg:member(), % The destination process of broadcasted messages

          %% Peers and Messages Management
          connections = #{}        :: #{connection() => #peer{}},
          missing = gb_sets:new()  :: gb_sets:set(message_id()),
          ihave = #{}              :: #{message_id() => message()},
          wehave = gb_sets:empty() :: gb_sets:set(message_id()),

          %% Internal Event Scheduler
          schedule = #schedule{} :: #schedule{},

          %% Protocol Parameters
          gossip_wait_timeout     :: timeout(),
          ihave_retention_period  :: timeout(),
          wehave_retention_period :: timeout(),
          ticktime                :: timeout()
        }).

-opaque tree() :: #?TREE{}.

-type peer_type() :: eager | lazy.

-type peer() :: #{
            pid        => ppg_peer:peer(),
            connection => ppg_hyparview:connection(),
            type       => peer_type(),
            nohaves    => non_neg_integer()
           }.

-type event() :: {gossip_wait_timeout, message_id(), connection()}
               | {ihave_expire, message_id()}
               | {wehave_expire, message_id()}
               | {tick, connection()}
               | {tack_timeout, connection()}.

-type message_id() :: reference().
-type message_type() :: 'APP' | 'SYS'.
-type message() :: {message_type(), ppg:message()}.

-type view() :: ppg_hyparview:view().

-type connection() :: ppg_hyparview:connection().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec default_options() -> [ppg:plumtree_option()].
default_options() ->
    [
     {gossip_wait_timeout, 50},
     {ihave_retention_period, 1 * 1000},
     {wehave_retention_period, 5 * 1000},
     {ticktime, 30 * 1000}
    ].

-spec new(ppg:member(), [ppg:plumtree_option()]) -> tree().
new(Member, Options0) ->
    _ = is_list(Options0) orelse error(badarg, [Member, Options0]),
    Options1 = Options0 ++ default_options(),
    Get =
        fun (Key, Validate) ->
                Value = proplists:get_value(Key, Options1),
                _ = Validate(Value) orelse error(badarg, [Member, Options0]),
                Value
        end,

    #?TREE{
        member                  = Member,
        gossip_wait_timeout     = Get(gossip_wait_timeout, fun ppg_util:is_timeout/1),
        ihave_retention_period  = Get(ihave_retention_period, fun ppg_util:is_timeout/1),
        wehave_retention_period = Get(wehave_retention_period, fun ppg_util:is_timeout/1),
        ticktime                = Get(ticktime, fun ppg_util:is_timeout/1)
       }.

-spec neighbor_up(ppg_peer:peer(), connection(), tree()) -> tree().
neighbor_up(PeerPid, Connection, Tree) ->
    Connections = maps:put(Connection, #peer{pid = PeerPid}, Tree#?TREE.connections),
    Schedule = schedule(Tree#?TREE.ticktime, {tick, Connection}, Tree#?TREE.schedule),
    Tree#?TREE{connections = Connections, schedule = Schedule}.

-spec neighbor_down(connection(), tree()) -> tree().
neighbor_down(Connection, Tree) ->
    Connections = maps:remove(Connection, Tree#?TREE.connections),
    Tree#?TREE{connections = Connections}.

-spec broadcast(ppg:message(), tree()) -> tree().
broadcast(Message, Tree) ->
    MsgId = make_ref(),
    push_and_deliver(MsgId, {'APP', Message}, undefined, Tree).

-spec system_broadcast(ppg:message(), tree()) -> tree().
system_broadcast(Message, Tree) ->
    MsgId = make_ref(),
    push_and_deliver(MsgId, {'SYS', Message}, undefined, Tree).

-spec get_member(tree()) -> ppg:member().
get_member(#?TREE{member = Member}) ->
    Member.

-spec get_peers(tree()) -> [peer()].
get_peers(#?TREE{connections = Connections}) ->
    maps:fold(
      fun (Connection, Peer, Acc) ->
              [#{
                  pid        => Peer#peer.pid,
                  connection => Connection,
                  type       => Peer#peer.type,
                  ihaves     => gb_sets:size(Peer#peer.ihave)
                } | Acc]
      end,
      [],
      Connections).

-spec handle_info(term(), view(), tree()) -> {ok, {view(), tree()}} | ignore.
handle_info({'GOSSIP', Connection, Arg}, View, Tree) -> {ok, {View, handle_gossip(Arg, Connection, Tree)}};
handle_info({'IHAVE',  Connection, Arg}, View, Tree) -> {ok, {View, handle_ihave(Arg, Connection, Tree)}};
handle_info({'PRUNE',  Connection},      View, Tree) -> {ok, {View, handle_prune(Connection, Tree)}};
handle_info({'GRAFT',  Connection, Arg}, View, Tree) -> {ok, {View, handle_graft(Arg, Connection, Tree)}};
handle_info({'TICK',   Connection},      View, Tree) -> {ok, {View, handle_tick(Connection, Tree)}};
handle_info({'TACK',   Connection},      View, Tree) -> {ok, {View, handle_tack(Connection, Tree)}};
handle_info({?MODULE, schedule},         View, Tree) -> {ok, handle_schedule(ppg_util:now_ms(), View, Tree)};
handle_info(_Info, _View, _Tree)                     -> ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_gossip({message_id(), message()}, connection(), tree()) -> tree().
handle_gossip({MsgId, Message}, Connection, Tree0) ->
    case maps:find(Connection, Tree0#?TREE.connections) of
        error      -> Tree0; % `Connection' has been disconnected
        {ok, Peer} ->
            case is_received(MsgId, Tree0) of
                false ->
                    %% New message
                    Tree1 = become(eager, Connection, Tree0),
                    push_and_deliver(MsgId, Message, Connection, Tree1);
                true ->
                    %% The message have been received from other peer
                    _ = Peer#peer.pid ! message_prune(Connection),
                    become(lazy, Connection, Tree0)
            end
    end.

-spec handle_ihave(message_id(), connection(), tree()) -> tree().
handle_ihave(MsgId, Connection, Tree) ->
    case maps:is_key(Connection, Tree#?TREE.connections) of
        false -> Tree; % `Connection' has been disconnected
        true  ->
            case is_received(MsgId, Tree) of
                false -> add_to_missing(MsgId, Connection, Tree);
                true  -> Tree
            end
    end.

-spec handle_prune(connection(), tree()) -> tree().
handle_prune(Connection, Tree) ->
    case maps:is_key(Connection, Tree#?TREE.connections) of
        false -> Tree; % `Connection' has been disconnected
        true  -> become(lazy, Connection, Tree)
    end.

-spec handle_graft(message_id(), connection(), tree()) -> tree().
handle_graft(MsgId, Connection, Tree) ->
    case maps:find(Connection, Tree#?TREE.connections) of
        error      -> Tree; % `Connection' has been disconnected
        {ok, Peer} ->
            case maps:find(MsgId, Tree#?TREE.ihave) of
                error ->
                    %% The message's retention period has expired.
                    _ = Peer#peer.pid ! message_prune(Connection),
                    become(lazy, Connection, Tree);
                {ok, Message} ->
                    _ = Peer#peer.pid ! message_gossip(Connection, MsgId, Message),
                    become(eager, Connection, Tree)
            end
    end.

-spec handle_tick(connection(), tree()) -> tree().
handle_tick(Connection, Tree) ->
    case maps:find(Connection, Tree#?TREE.connections) of
        error      -> Tree;
        {ok, Peer} ->
            _ = Peer#peer.pid ! message_tack(Connection),
            Tree
    end.

-spec handle_tack(connection(), tree()) -> tree().
handle_tack(Connection, Tree) ->
    case maps:find(Connection, Tree#?TREE.connections) of
        error      -> Tree;
        {ok, Peer} ->
            Connections = maps:put(Connection, Peer#peer{in_tick = false}, Tree#?TREE.connections),
            Schedule = schedule(Tree#?TREE.ticktime, {tick, Connection}, Tree#?TREE.schedule),
            Tree#?TREE{connections = Connections, schedule = Schedule}
    end.

-spec handle_schedule(ppg_util:milliseconds(), view(), tree()) -> {view(), tree()}.
handle_schedule(Now, View0, Tree0) ->
    Schedule = Tree0#?TREE.schedule,
    case ppg_pq:out(Schedule#schedule.queue) of
        empty                          -> {View0, Tree0};
        {{Time, _}, _} when Time > Now ->
            %% The execution time of a next event is not reached.
            %% For overload protection, the maximum execution count of the function is limited to one hundred per second.
            After = max(Time - Now, 10),
            Timer = erlang:send_after(After, self(), {?MODULE, schedule}),
            {View0, Tree0#?TREE{schedule = Schedule#schedule{timer = Timer}}};
        {{_, {tick, Connection}}, Queue} ->
            case maps:find(Connection, Tree0#?TREE.connections) of
                error      -> handle_schedule(Now, View0, Tree0#?TREE{schedule = Schedule#schedule{queue = Queue}});
                {ok, Peer} ->
                    _ = Peer#peer.pid ! message_tick(Connection),
                    Connections = maps:put(Connection, Peer#peer{in_tick = true}, Tree0#?TREE.connections),
                    Schedule1 = schedule(Tree0#?TREE.ihave_retention_period, {tack_timeout, Connection},
                                         Schedule#schedule{queue = Queue}),
                    handle_schedule(Now, View0, Tree0#?TREE{connections = Connections, schedule = Schedule1})
            end;
        {{_, {tack_timeout, Connection}}, Queue} ->
            case maps:find(Connection, Tree0#?TREE.connections) of
                {ok, #peer{in_tick = true, pid = Pid}} ->
                    {Tree1, View1} = ppg_hyparview:disconnect(Pid, Tree0, View0),
                    handle_schedule(Now, View1, Tree1#?TREE{schedule = Schedule#schedule{queue = Queue}});
                _ ->
                    handle_schedule(Now, View0, Tree0#?TREE{schedule = Schedule#schedule{queue = Queue}})
            end;
        {{_, {ihave_expire, MsgId}}, Queue} ->
            Tree1 = move_to_wehave(MsgId, Tree0),
            handle_schedule(Now, View0, Tree1#?TREE{schedule = Schedule#schedule{queue = Queue}});
        {{_, {wehave_expire, MsgId}}, Queue} ->
            Wehave = gb_sets:delete(MsgId, Tree0#?TREE.wehave),
            handle_schedule(Now, View0, Tree0#?TREE{schedule = Schedule#schedule{queue = Queue}, wehave = Wehave});
        {{_, {gossip_wait_timeout, MsgId, Connection}}, Queue} ->
            Tree1 =
                case gb_sets:is_member(MsgId,Tree0#?TREE.missing) andalso maps:find(Connection, Tree0#?TREE.connections) of
                    {ok, Peer} ->
                        _ = Peer#peer.pid ! message_graft(Connection, MsgId),
                        become(eager, Connection, Tree0);
                    _ ->
                        Tree0
                end,
            handle_schedule(Now, View0, Tree1#?TREE{schedule = Schedule#schedule{queue = Queue}})
    end.

-spec schedule(timeout(), event(), #schedule{}) -> #schedule{}.
schedule(infinity, _,  Schedule) -> Schedule;
schedule(After, Event, Schedule) ->
    ExecutionTime = ppg_util:now_ms() + After,
    Queue = ppg_pq:in({ExecutionTime, Event}, Schedule#schedule.queue),
    case ppg_pq:peek(Schedule#schedule.queue) of
        {Next, _} when Next =< (ExecutionTime + 10) ->
            Schedule#schedule{queue = Queue};
        _ ->
            Timer = ppg_util:cancel_and_send_after(Schedule#schedule.timer, After, self(), {?MODULE, schedule}),
            Schedule#schedule{queue = Queue, timer = Timer}
    end.

-spec is_received(message_id(), tree()) -> boolean().
is_received(MsgId, Tree) ->
    maps:is_key(MsgId, Tree#?TREE.ihave) orelse gb_sets:is_member(MsgId, Tree#?TREE.wehave).

-spec become(peer_type(), connection(), tree()) -> tree().
become(Type, Connection, Tree) ->
    Peer = maps:get(Connection, Tree#?TREE.connections),
    case Peer#peer.type =:= Type of
        true  -> Tree;
        false ->
            Connections = maps:put(Connection, Peer#peer{type = Type}, Tree#?TREE.connections),
            Tree#?TREE{connections = Connections}
    end.

-spec push_and_deliver(message_id(), message(), connection()|undefined, tree()) -> tree().
push_and_deliver(MsgId, Message, Sender, Tree) ->
    %% Delivers the message
    _ = case Message of
            {'APP', Data} -> Tree#?TREE.member ! Data;
            {'SYS', Data} -> self() ! Data
        end,
    Tree1 = move_to_ihave(MsgId, Message, Tree),
    push(MsgId, Message, Sender, Tree1).

-spec push(message_id(), message(), connection()|undefined, tree()) -> tree().
push(MsgId, Message, Sender, Tree) ->
    Connections =
        maps:map(
          fun (C, P) when C =:= Sender ->
                  P;
              (C, P = #peer{pid = Pid, type = Type, ihave = Ihave}) ->
                  case gb_sets:is_member(MsgId, Ihave) of
                      true  -> P#peer{ihave = gb_sets:delete(MsgId, Ihave)};
                      false ->
                          _ = Pid ! case Type =:= eager of
                                        true  -> message_gossip(C, MsgId, Message);
                                        false -> message_ihave(C, MsgId)
                                    end,
                          P
                  end
          end,
          Tree#?TREE.connections),
    Tree#?TREE{connections = Connections}.

-spec move_to_ihave(message_id(), message(), tree()) -> tree().
move_to_ihave(MsgId, Message, Tree) ->
    Missing = gb_sets:delete_any(MsgId, Tree#?TREE.missing),
    Ihave = maps:put(MsgId, Message, Tree#?TREE.ihave),
    Schedule = schedule(Tree#?TREE.ihave_retention_period, {ihave_expire, MsgId}, Tree#?TREE.schedule),
    Tree#?TREE{missing = Missing, ihave = Ihave, schedule = Schedule}.

-spec move_to_wehave(message_id(), tree()) -> tree().
move_to_wehave(MsgId, Tree) ->
    Ihave = maps:remove(MsgId, Tree#?TREE.ihave),
    Wehave = gb_sets:add(MsgId, Tree#?TREE.wehave),
    Schedule = schedule(Tree#?TREE.wehave_retention_period, {wehave_expire, MsgId}, Tree#?TREE.schedule),
    Tree#?TREE{ihave = Ihave, wehave = Wehave, schedule = Schedule}.

-spec add_to_missing(message_id(), connection(), tree()) -> tree().
add_to_missing(MsgId, Connection, Tree) ->
    Peer = maps:get(Connection, Tree#?TREE.connections),
    Connections = maps:put(Connection, Peer#peer{ihave = gb_sets:add(MsgId, Peer#peer.ihave)}, Tree#?TREE.connections),

    Missing = gb_sets:add(MsgId, Tree#?TREE.missing),
    After = Tree#?TREE.gossip_wait_timeout * ppg_maps:count(fun (_, P) -> gb_sets:is_member(MsgId, P#peer.ihave) end, Connections),
    Schedule = schedule(After, {gossip_wait_timeout, MsgId, Connection}, Tree#?TREE.schedule),
    Tree#?TREE{missing = Missing, schedule = Schedule, connections = Connections}.

-spec message_gossip(connection(), message_id(), message()) -> {'GOSSIP', connection(), {message_id(), message()}}.
message_gossip(Connection, MsgId, Message) ->
    {'GOSSIP', Connection, {MsgId, Message}}.

-spec message_ihave(connection(), message_id()) -> {'IHAVE', connection(), message_id()}.
message_ihave(Connection, MsgId) ->
    {'IHAVE', Connection, MsgId}.

-spec message_graft(connection(), message_id()) -> {'GRAFT', connection(), message_id()}.
message_graft(Connection, MsgId) ->
    {'GRAFT', Connection, MsgId}.

-spec message_prune(connection()) -> {'PRUNE', connection()}.
message_prune(Connection) ->
    {'PRUNE', Connection}.

-spec message_tick(connection()) -> {'TICK', connection()}.
message_tick(Connection) ->
    {'TICK', Connection}.

-spec message_tack(connection()) -> {'TACK', connection()}.
message_tack(Connection) ->
    {'TACK', Connection}.
