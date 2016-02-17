%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
%% @private
-module(ppg_plumtree).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/2]).
-export([broadcast/2]).
-export([handle_info/2]).
-export([get_entry/1]).
-export([neighbor_up/3]).
-export([neighbor_down/3]).

-export_type([tree/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(TAG_GOSSIP, 'GOSSIP').
-define(TAG_IHAVE, 'IHAVE').
-define(TAG_PRUNE, 'PRUNE').
-define(TAG_GRAFT, 'GRAFT').

-define(TREE, ?MODULE).

-record(peer,
        {
          pid                      :: ppg_peer:peer(),
          type = eager             :: eager | lazy,
          nohave = gb_sets:empty() :: gb_sets:set(msg_id()) % TODO: 一定サイズを超えたらpeerをkill|disconnectする
        }).

-record(schedule,
        {
          timer = make_ref() :: reference(),
          queue = pairing_heaps:new() :: pairing_heaps:heap({milliseconds(), {ihave|done, msg_id()}})
        }).

-record(?TREE,
        {
          member                      :: ppg:member(),
          connections                 :: #{connection() => #peer{}},
          missing = #{}               :: #{msg_id() => IHave::[connection()]},
          received = #{}              :: #{msg_id() => message()},
          delivered = gb_sets:empty() :: gb_sets:set(msg_id()),
          schedule = #schedule{}      :: #schedule{},

          max_nohave_count = 100 :: pos_integer(),
          done_timeout = 1000 :: milliseconds(), % XXX: name
          ihave_timeout = 100 :: timeout()
        }).

-opaque tree() :: #?TREE{}.

-type msg_id() :: reference().
-type message() :: term().

-type milliseconds() :: non_neg_integer(). % TODO: timeout()

-type connection() :: ppg_hyparview:connection().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new(ppg:member(), [{ppg_peer:peer(), connection()}]) -> tree().
new(Member, Peers) ->
    #?TREE{
        member      = Member,
        connections = maps:from_list([{C, #peer{pid = P}}|| {P, C} <- Peers])
       }.

-spec neighbor_up(ppg_peer:peer(), connection(), tree()) -> tree().
neighbor_up(PeerPid, Connection, Tree) ->
    Nohave =
        maps:fold(fun (MsgId, _, Acc) ->
                          _ = PeerPid ! message_ihave(Connection, MsgId),
                          gb_sets:add(MsgId, Acc)
                  end,
                  gb_sets:empty(),
                  Tree#?TREE.received),
    Connections = maps:put(Connection, #peer{pid = PeerPid, nohave = Nohave}, Tree#?TREE.connections),
    Tree#?TREE{connections = Connections}.

-spec neighbor_down(ppg_peer:peer(), connection(), tree()) -> tree().
neighbor_down(_, Connection, Tree) ->
    Connections = maps:remove(Connection, Tree#?TREE.connections),
    {Received, Delivered, Schedule} =
        maps:fold(
          fun (MsgId, Message, {AccR, AccD, AccS}) ->
                  case is_delivered(MsgId, Connections) of
                      true  -> {AccR, gb_sets:add(MsgId, AccD), schedule(Tree#?TREE.done_timeout, {done, MsgId}, AccS)};
                      false -> {maps:put(MsgId, Message, AccR), AccD, AccS}
                  end
          end,
          {#{}, Tree#?TREE.delivered, Tree#?TREE.schedule},
          Tree#?TREE.received),
    Missing =
        ppg_maps:filtermap(
          fun (_, IhaveList) ->
                  case lists:delete(Connection, IhaveList) of
                      []   -> false;
                      List -> {true, List}
                  end
          end,
          Tree#?TREE.missing),
    Tree#?TREE{connections = Connections, received = Received, missing = Missing, delivered = Delivered, schedule = Schedule}.

-spec broadcast(term(), tree()) -> tree().
broadcast(Message, Tree0) ->
    MsgId = make_ref(),
    Tree1 = push(MsgId, Message, undefined, Tree0),
    Tree2 = received(MsgId, Message, [], Tree1),
    deliver(Message, Tree2).

-spec get_entry(tree()) -> Todo::term(). % TODO: Return message history size
get_entry(Tree) ->
    {self(), Tree#?TREE.member, Tree#?TREE.connections}.

-spec handle_info(term(), tree()) -> {ok, tree()} | ignore.
handle_info({?TAG_GOSSIP, Connection, Arg}, Tree) -> handle_gossip(Arg, Connection, Tree);
handle_info({?TAG_IHAVE,  Connection, Arg}, Tree) -> handle_ihave(Arg, Connection, Tree);
handle_info({?TAG_PRUNE,  Connection},      Tree) -> handle_prune(Connection, Tree);
handle_info({?TAG_GRAFT,  Connection, Arg}, Tree) -> handle_graft(Arg, Connection, Tree);
handle_info({?MODULE, schedule},            Tree) -> handle_schedule(now_ms(), Tree);
handle_info(_Info, _Tree)                         -> ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_gossip({msg_id(), message()}, connection(), tree()) -> {ok, tree()}.
handle_gossip({MsgId, Message}, Connection, Tree0) ->
    case maps:find(Connection, Tree0#?TREE.connections) of
        error      -> {ok, Tree0}; % `Connection' has been disconnected
        {ok, Peer} ->
            case message_type(MsgId, Tree0) of
                delivered ->
                    %% 配送完了後に追加されたノードからのメッセージ
                    _ = Peer#peer.pid ! message_ihave(Connection, MsgId),
                    {ok, Tree0};
                received ->
                    %% 別の経路からすでに受信済み
                    _ = Peer#peer.pid ! message_ihave(Connection, MsgId),
                    _ = Peer#peer.pid ! message_prune(Connection),
                    Tree1 = become(lazy, Connection, Tree0),
                    Tree2 = remove_from_nohave(MsgId, Connection, Tree1),
                    {ok, Tree2};
                _ -> % missing | new
                    Tree1 = deliver(Message, Tree0),
                    Tree2 = push(MsgId, Message, Connection, Tree1),
                    Tree3 = become(eager, Connection, Tree2),
                    Tree4 = received(MsgId, Message, [Connection], Tree3),
                    {ok, Tree4}
            end
    end.

-spec handle_ihave(msg_id(), connection(), tree()) -> {ok, tree()}.
handle_ihave(MsgId, Connection, Tree) ->
    case maps:is_key(Connection, Tree#?TREE.connections) of
        false -> {ok, Tree}; % `Connection' has been disconnected
        true  ->
            case message_type(MsgId, Tree) of
                delivered -> {ok, Tree}; % TODO: reply ihave(?) => graft時にnohaveに含まれないようになっているなら不要
                received  -> {ok, remove_from_nohave(MsgId, Connection, Tree)};
                _         -> {ok, missing(MsgId, Connection, Tree)}
            end
    end.

-spec handle_prune(connection(), tree()) -> {ok, tree()}.
handle_prune(Connection, Tree) ->
    case maps:is_key(Connection, Tree#?TREE.connections) of
        false -> {ok, Tree}; % `Connection' has been disconnected
        true  -> {ok, become(lazy, Connection, Tree)}
    end.

-spec handle_graft(msg_id(), connection(), tree()) -> {ok, tree()}.
handle_graft(MsgId, Connection, Tree0) ->
    case maps:find(Connection, Tree0#?TREE.connections) of
        error      -> {ok, Tree0}; % `Connection' has been disconnected
        {ok, Peer} ->
            Tree1 = become(eager, Connection, Tree0),
            case maps:find(MsgId, Tree1#?TREE.received) of
                error         -> {ok, Tree1}; % maybe timed out
                {ok, Message} ->
                    _ = Peer#peer.pid ! message_gossip(Connection, MsgId, Message),
                    {ok, Tree1}
            end
    end.

-spec is_delivered(msg_id(), #{connection() => #peer{}}) -> boolean().
is_delivered(MsgId, Connections) ->
    not ppg_maps:any(fun (_, #peer{nohave = Nohave}) -> gb_sets:is_member(MsgId, Nohave) end, Connections).

-spec schedule(timeout(), term(), #schedule{}) -> #schedule{}.
schedule(After, Task, Schedule) ->
    Time = now_ms() + After,
    Queue = pairing_heaps:in({Time, Task}, Schedule#schedule.queue),
    case pairing_heaps:peek(Schedule#schedule.queue) of
        {{Next, _}, _} when Next =< Time ->
            Schedule#schedule{queue = Queue};
        _ ->
            _ = erlang:cancel_timer(Schedule#schedule.timer),
            _ = receive {?MODULE, schedule} -> ok after 0 -> ok end,
            Timer = erlang:send_after(After + 1, self(), {?MODULE, schedule}),
            Schedule#schedule{queue = Queue, timer = Timer}
    end.

%% TODO:
-spec message_type(msg_id(), tree()) -> new | missing | received | delivered.
message_type(MsgId, Tree) ->
    case gb_sets:is_member(MsgId, Tree#?TREE.delivered) of
        true  -> delivered;
        false ->
            case maps:is_key(MsgId, Tree#?TREE.missing) of
                true  -> missing;
                false ->
                    case maps:is_key(MsgId, Tree#?TREE.received) of
                        true  -> received;
                        false -> new
                    end
            end
    end.

-spec become(lazy|eager, connection(), tree()) -> tree().
become(Type, Connection, Tree) ->
    Peer = maps:get(Connection, Tree#?TREE.connections),
    case Peer#peer.type =:= Type of
        true  -> Tree;
        false ->
            Connections = maps:put(Connection, Peer#peer{type = Type}, Tree#?TREE.connections),
            Tree#?TREE{connections = Connections}
    end.

%% TODO:
-spec remove_from_nohave(msg_id(), connection(), tree()) -> tree().
remove_from_nohave(MsgId, Connection, Tree) ->
    Peer = maps:get(Connection, Tree#?TREE.connections),
    case gb_sets:is_member(MsgId, Peer#peer.nohave) of
        false -> Tree;
        true  ->
            Nohave = gb_sets:delete(MsgId, Peer#peer.nohave),
            Connections = maps:put(Connection, Peer#peer{nohave = Nohave}, Tree#?TREE.connections),
            case is_delivered(MsgId, Connections) of
                false -> Tree#?TREE{connections = Connections};
                true  ->
                    Delivered = gb_sets:add(MsgId, Tree#?TREE.delivered),
                    Schedule = schedule(Tree#?TREE.done_timeout, {done, MsgId}, Tree#?TREE.schedule),
                    Received = maps:remove(MsgId, Tree#?TREE.received),
                    Tree#?TREE{connections = Connections, received = Received, delivered = Delivered, schedule = Schedule}
            end
    end.

-spec received(msg_id(), message(), [connection()], tree()) -> tree().
received(MsgId, Message, MaybeConnection, Tree) ->
    {Missing, IhaveList} =
        case maps:find(MsgId, Tree#?TREE.missing) of
            error      -> {Tree#?TREE.missing, MaybeConnection};
            {ok, List} -> {maps:remove(MsgId, Tree#?TREE.missing), MaybeConnection ++ List}
        end,
    case maps:size(Tree#?TREE.connections) =:= length(IhaveList) of
        true ->
            Delivered = gb_sets:add(MsgId, Tree#?TREE.delivered),
            Schedule = schedule(Tree#?TREE.done_timeout, {done, MsgId}, Tree#?TREE.schedule),
            Tree#?TREE{missing = Missing, delivered = Delivered, schedule = Schedule};
        false ->
            Connections =
                maps:map(
                  fun (C, P) ->
                          case lists:member(C, IhaveList) of
                              true  -> P;
                              false -> P#peer{nohave = gb_sets:add(MsgId, P#peer.nohave)}
                          end
                  end,
                  Tree#?TREE.connections),
            Received = maps:put(MsgId, Message, Tree#?TREE.received),
            Tree#?TREE{connections = Connections, missing = Missing, received = Received}
    end.

-spec missing(msg_id(), connection(), tree()) -> tree().
missing(MsgId, Connection, Tree) ->
    {Missing, Schedule} =
        case maps:find(MsgId, Tree#?TREE.missing) of
            error      -> {maps:put(MsgId, [Connection], Tree#?TREE.missing),
                           schedule(Tree#?TREE.ihave_timeout, {ihave, MsgId}, Tree#?TREE.schedule)};
            {ok, List} -> {maps:put(MsgId, List ++ [Connection], Tree#?TREE.missing), Tree#?TREE.schedule}
        end,
    Tree#?TREE{missing = Missing, schedule = Schedule}.

-spec handle_schedule(milliseconds(), tree()) -> {ok, tree()}.
handle_schedule(Now, Tree) ->
    Schedule = Tree#?TREE.schedule,
    case pairing_heaps:out(Schedule#schedule.queue) of
        empty                          -> {ok, Tree};
        {{Time, _}, _} when Time > Now ->
            _ = erlang:cancel_timer(Schedule#schedule.timer), % 念のため
            _ = receive {?MODULE, schedule} -> ok after 0 -> ok end,
            Timer = erlang:send_after(Time - Now + 1, self(), {?MODULE, schedule}),
            {ok, Tree#?TREE{schedule = Schedule#schedule{timer = Timer}}};
        {{_, {done, Id}}, Queue} ->
            {ok, Tree1} = handle_done(Id, Tree),
            handle_schedule(Now, Tree1#?TREE{schedule = Schedule#schedule{queue = Queue}});
        {{_, {ihave, Id}}, Queue} ->
            {ok, Tree1} = handle_ihave_timeout(Id, Tree),
            handle_schedule(Now, Tree1#?TREE{schedule = Schedule#schedule{queue = Queue}})
    end.

-spec handle_done(msg_id(), tree()) -> {ok, tree()}.
handle_done(MsgId, Tree) ->
    Delivered = gb_sets:delete(MsgId, Tree#?TREE.delivered),
    {ok, Tree#?TREE{delivered = Delivered}}.

-spec handle_ihave_timeout(msg_id(), tree()) -> {ok, tree()}.
handle_ihave_timeout(MsgId, Tree0) ->
    case maps:find(MsgId, Tree0#?TREE.missing) of
        error                          -> {ok, Tree0};
        {ok, [Connection | IhaveList]} ->
            Peer = maps:get(Connection, Tree0#?TREE.connections),
            _ = Peer#peer.pid ! message_graft(Connection, MsgId),
            Tree1 = become(eager, Connection, Tree0),
            case IhaveList of
                [] ->
                    Missing = maps:remove(MsgId, Tree1#?TREE.missing),
                    {ok, Tree1#?TREE{missing = Missing}};
                _  ->
                    Schedule = schedule(Tree1#?TREE.ihave_timeout, {ihave, MsgId}, Tree1#?TREE.schedule),
                    Missing = maps:put(MsgId, IhaveList, Tree1#?TREE.missing),
                    {ok, Tree1#?TREE{missing = Missing, schedule = Schedule}}
            end
    end.

-spec push(msg_id(), term(), connection()|undefined, tree()) -> tree().
push(MsgId, Message, Sender, Tree) ->
    ok = ppg_maps:foreach(
           fun (C, #peer{pid = Pid, type = Type}) ->
                   Pid ! case Type =:= eager andalso C =/= Sender of
                             true  -> message_gossip(C, MsgId, Message);
                             false -> message_ihave(C, MsgId)
                         end
           end,
           Tree#?TREE.connections),
    Tree.

-spec deliver(term(), tree()) -> tree().
deliver(Message, Tree) ->
    %% TODO:
    _ = case Message of
            {'SYSTEM', get_graph, {Ref, From}} -> % TODO:
                From ! {Ref, get_entry(Tree)};
            {'INTERNAL', X} -> % TODO:
                self() ! X;
            _ ->
                Tree#?TREE.member ! Message
        end,
    Tree.

-spec now_ms() -> milliseconds().
now_ms() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000 * 1000 * 1000) + (Sec * 1000) + (Micro div 1000).

-spec message_gossip(connection(), msg_id(), message()) -> {?TAG_GOSSIP, connection(), {msg_id(), message()}}.
message_gossip(Connection, MsgId, Message) ->
    {?TAG_GOSSIP, Connection, {MsgId, Message}}.

-spec message_ihave(connection(), msg_id()) -> {?TAG_IHAVE, connection(), msg_id()}.
message_ihave(Connection, MsgId) ->
    {?TAG_IHAVE, Connection, MsgId}.

-spec message_graft(connection(), msg_id()) -> {?TAG_GRAFT, connection(), msg_id()}.
message_graft(Connection, MsgId) ->
    {?TAG_GRAFT, Connection, MsgId}.

-spec message_prune(connection()) -> {?TAG_PRUNE, connection()}.
message_prune(Connection) ->
    {?TAG_PRUNE, Connection}.
