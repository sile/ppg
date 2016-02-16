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
-export([neighbor_up/2]).
-export([neighbor_down/2]).

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
          member                            :: ppg:member(),
          peers = #{}                       :: #{peer_id() => #peer{}},
          missing = #{}                     :: #{msg_id() => IHave::[peer_id()]},
          received = #{}                    :: #{msg_id() => message()},
          delivered = gb_sets:empty()       :: gb_sets:set(msg_id()),
          schedule = #schedule{}            :: #schedule{},

          max_nohave_count = 100 :: pos_integer(),
          done_timeout = 1000 :: milliseconds(), % XXX: name
          ihave_timeout = 100 :: timeout()
        }).

-opaque tree() :: #?TREE{}.

-type msg_id() :: reference().
-type message() :: term().

-type milliseconds() :: non_neg_integer().

-type peer_id() :: {ppg_hyparview:connection_id(), ppg_peer:peer()}. % TODO: もっとサイズを節約したい

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new(ppg:member(), [{peer_id(), ppg_peer:peer()}]) -> tree().
new(Member, Peers) ->
    #?TREE{
        member = Member,
        peers = maps:from_list([{P, #peer{}} || P <- Peers])
       }.

-spec neighbor_up(peer_id(), tree()) -> tree().
neighbor_up(Peer, Tree) ->
    Nohave =
        maps:fold(fun (MsgId, _, Acc) ->
                          ok = send_ihave(Peer, MsgId),
                          gb_sets:add(MsgId, Acc)
                  end,
                  gb_sets:empty(),
                  Tree#?TREE.received),
    Peers = maps:put(Peer, #peer{nohave = Nohave}, Tree#?TREE.peers),
    Tree#?TREE{peers = Peers}.

-spec neighbor_down(peer_id(), tree()) -> tree().
neighbor_down(Peer, Tree) ->
    Peers = maps:remove(Peer, Tree#?TREE.peers),
    {Received, Delivered, Schedule} =
        maps:fold(
          fun (MsgId, Message, {AccR, AccD, AccS}) ->
                  case is_delivered(MsgId, Peers) of
                      true  -> {AccR, gb_sets:add(MsgId, AccD), schedule(Tree#?TREE.done_timeout, {done, MsgId}, AccS)};
                      false -> {maps:put(MsgId, Message, AccR), AccD, AccS}
                  end
          end,
          {#{}, Tree#?TREE.delivered, Tree#?TREE.schedule},
          Tree#?TREE.received),
    Missing =
        maps:fold(
          fun (_, [P], Acc) when P =:= Peer -> Acc;
              (MsgId, IhaveList, Acc)       -> maps:put(MsgId, lists:delete(Peer, IhaveList), Acc)
          end,
          #{},
          Tree#?TREE.missing),
    Tree#?TREE{peers = Peers, received = Received, missing = Missing, delivered = Delivered, schedule = Schedule}.

-spec broadcast(term(), tree()) -> tree().
broadcast(Message, Tree0) ->
    MsgId = make_ref(),
    Tree1 = push(MsgId, Message, undefined, Tree0),
    Tree2 = received(MsgId, Message, [], Tree1),
    deliver(Message, Tree2).

-spec get_entry(tree()) -> Todo::term(). % TODO: Return message history size
get_entry(Tree) ->
    {self(), Tree#?TREE.member, Tree#?TREE.peers}.

-spec handle_info(term(), tree()) -> {ok, tree()} | ignore.
handle_info({?TAG_GOSSIP, Arg}, Tree) -> handle_gossip(Arg, Tree);
handle_info({?TAG_IHAVE, Arg}, Tree) -> handle_ihave(Arg, Tree);
handle_info({?TAG_PRUNE, Arg}, Tree) -> handle_prune(Arg, Tree);
handle_info({?TAG_GRAFT, Arg}, Tree) -> handle_graft(Arg, Tree);
handle_info({?MODULE, schedule}, Tree) -> handle_schedule(now_ms(), Tree);
handle_info(_Info, _Tree) ->
    ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_gossip({msg_id(), message(), peer_id()}, tree()) -> {ok, tree()}.
handle_gossip({MsgId, Message, Sender}, Tree0) ->
    case maps:is_key(Sender, Tree0#?TREE.peers) of
        false -> {ok, Tree0}; % From disconnected channel
        true  ->
            case message_type(MsgId, Tree0) of
                delivered ->
                    %% 配送完了後に追加されたノードからのメッセージ
                    ok = send_ihave(Sender, MsgId),
                    {ok, Tree0};
                received ->
                    %% 別の経路からすでに受信済み
                    ok = send_ihave(Sender, MsgId),
                    ok = send_prune(Sender),
                    Tree1 = become_lazy(Sender, Tree0),
                    Tree2 = remove_from_nohave(MsgId, Sender, Tree1),
                    {ok, Tree2};
                _ -> % missing | new
                    Tree1 = deliver(Message, Tree0),
                    Tree2 = push(MsgId, Message, Sender, Tree1),
                    Tree3 = become_eager(Sender, Tree2),
                    Tree4 = received(MsgId, Message, [Sender], Tree3),
                    {ok, Tree4}
            end
    end.

-spec handle_ihave({msg_id(), peer_id()}, tree()) -> {ok, tree()}.
handle_ihave({MsgId, Sender}, Tree) ->
    case maps:is_key(Sender, Tree#?TREE.peers) of
        false -> {ok, Tree}; % TODO: note
        true  ->
            case message_type(MsgId, Tree) of
                delivered -> {ok, Tree}; % TODO: reply ihave(?) => graft時にnohaveに含まれないようになっているなら不要
                received  -> {ok, remove_from_nohave(MsgId, Sender, Tree)};
                _         -> {ok, missing(MsgId, Sender, Tree)}
            end
    end.

-spec handle_prune(peer_id(), tree()) -> {ok, tree()}.
handle_prune(Sender, Tree) ->
    case maps:is_key(Sender, Tree#?TREE.peers) of
        false -> {ok, Tree}; % TODO: note
        true  -> {ok, become_lazy(Sender, Tree)}
    end.

-spec handle_graft({msg_id(), peer_id()}, tree()) -> {ok, tree()}.
handle_graft({MsgId, Sender}, Tree0) ->
    case maps:is_key(Sender, Tree0#?TREE.peers) of
        false -> {ok, Tree0}; % TODO: note
        true  ->
            Tree1 = become_eager(Sender, Tree0),
            case maps:find(MsgId, Tree1#?TREE.received) of
                error         -> {ok, Tree1}; % maybe timed out
                {ok, Message} ->
                    ok = send_gossip(Sender, MsgId, Message),
                    {ok, Tree1}
            end
    end.

-spec is_delivered(msg_id(), #{ppg_peer:peer() => #peer{}}) -> boolean().
is_delivered(Id, Peers) ->
    maps:fold(fun (_, _, false)                  -> false;
                  (_, #peer{nohave = Nohave}, _) -> not gb_sets:is_member(Id, Nohave)
              end,
              true,
              Peers).

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

-spec become_lazy(peer_id(), tree()) -> tree().
become_lazy(Peer, Tree) ->
    #{Peer := State} = Tree#?TREE.peers,
    case State#peer.type =:= lazy of
        true  -> Tree;
        false ->
            Peers = maps:put(Peer, State#peer{type = lazy}, Tree#?TREE.peers),
            Tree#?TREE{peers = Peers}
    end.

-spec become_eager(peer_id(), tree()) -> tree().
become_eager(Peer, Tree) ->
    #{Peer := State} = Tree#?TREE.peers,
    case State#peer.type =:= eager of
        true  -> Tree;
        false ->
            Peers = maps:put(Peer, State#peer{type = eager}, Tree#?TREE.peers),
            Tree#?TREE{peers = Peers}
    end.

%% TODO:
-spec remove_from_nohave(msg_id(), peer_id(), tree()) -> tree().
remove_from_nohave(MsgId, Peer, Tree) ->
    #{Peer := State} = Tree#?TREE.peers,
    case gb_sets:is_member(MsgId, State#peer.nohave) of
        false -> Tree;
        true  ->
            Peers = maps:put(Peer, State#peer{nohave = gb_sets:delete(MsgId, State#peer.nohave)}, Tree#?TREE.peers),
            case is_delivered(MsgId, Peers) of
                false -> Tree#?TREE{peers = Peers};
                true  ->
                    Delivered = gb_sets:add(MsgId, Tree#?TREE.delivered),
                    Schedule = schedule(Tree#?TREE.done_timeout, {done, MsgId}, Tree#?TREE.schedule),
                    Received = maps:remove(MsgId, Tree#?TREE.received),
                    Tree#?TREE{peers = Peers, received = Received, delivered = Delivered, schedule = Schedule}
            end
    end.

-spec received(msg_id(), message(), [peer_id()], tree()) -> tree().
received(MsgId, Message, MaybeSender, Tree) ->
    {Missing, IhaveList} =
        case maps:find(MsgId, Tree#?TREE.missing) of
            error      -> {Tree#?TREE.missing, MaybeSender};
            {ok, List} -> {maps:remove(MsgId, Tree#?TREE.missing), MaybeSender ++ List}
        end,
    case maps:size(Tree#?TREE.peers) =:= length(IhaveList) of
        true ->
            Delivered = gb_sets:add(MsgId, Tree#?TREE.delivered),
            Schedule = schedule(Tree#?TREE.done_timeout, {done, MsgId}, Tree#?TREE.schedule),
            Tree#?TREE{missing = Missing, delivered = Delivered, schedule = Schedule};
        false ->
            Peers =
                maps:map(
                  fun (P, S) ->
                          case lists:member(P, IhaveList) of
                              true  -> S;
                              false -> S#peer{nohave = gb_sets:add(MsgId, S#peer.nohave)}
                          end
                  end,
                  Tree#?TREE.peers),
            Received = maps:put(MsgId, Message, Tree#?TREE.received),
            Tree#?TREE{peers = Peers, missing = Missing, received = Received}
    end.

-spec missing(msg_id(), peer_id(), tree()) -> tree().
missing(MsgId, Sender, Tree) ->
    {Missing, Schedule} =
        case maps:find(MsgId, Tree#?TREE.missing) of
            error      -> {maps:put(MsgId, [Sender], Tree#?TREE.missing),
                           schedule(Tree#?TREE.ihave_timeout, {ihave, MsgId}, Tree#?TREE.schedule)};
            {ok, List} -> {maps:put(MsgId, List ++ [Sender], Tree#?TREE.missing), Tree#?TREE.schedule}
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
handle_done(Id, Tree) ->
    Delivered = gb_sets:delete(Id, Tree#?TREE.delivered),
    {ok, Tree#?TREE{delivered = Delivered}}.

-spec handle_ihave_timeout(msg_id(), tree()) -> {ok, tree()}.
handle_ihave_timeout(Id, Tree0) ->
    case maps:find(Id, Tree0#?TREE.missing) of
        error                    -> {ok, Tree0};
        {ok, [Peer | IhaveList]} ->
            ok = send_graft(Peer, Id),
            Tree1 = become_eager(Peer, Tree0),
            case IhaveList of
                [] ->
                    Missing = maps:remove(Id, Tree1#?TREE.missing),
                    {ok, Tree1#?TREE{missing = Missing}};
                _  ->
                    Schedule = schedule(Tree1#?TREE.ihave_timeout, {ihave, Id}, Tree1#?TREE.schedule),
                    Missing = maps:put(Id, IhaveList, Tree1#?TREE.missing),
                    {ok, Tree1#?TREE{missing = Missing, schedule = Schedule}}
            end
    end.

-spec push(msg_id(), term(), peer_id()|undefined, tree()) -> tree().
push(MsgId, Message, Sender, Tree) ->
    ok = ppg_maps:foreach(
           fun (P, #peer{type = eager}) when P =/= Sender -> send_gossip(P, MsgId, Message);
               (P, _)                                     -> send_ihave(P, MsgId)
           end,
           Tree#?TREE.peers),
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

-spec send_graft(peer_id(), msg_id()) -> ok.
send_graft({Id, Peer}, MsgId) ->
    _ = Peer ! {?TAG_GRAFT, {MsgId, {Id, self()}}},
    ok.

-spec send_gossip(peer_id(), msg_id(), message()) -> ok.
send_gossip({Id, Peer}, MsgId, Message) ->
    _ = Peer ! {?TAG_GOSSIP, {MsgId, Message, {Id, self()}}},
    ok.

-spec send_ihave(peer_id(), msg_id()) -> ok.
send_ihave({Id, Peer}, MsgId) ->
    _ = Peer ! {?TAG_IHAVE, {MsgId, {Id, self()}}},
    ok.

-spec send_prune(peer_id()) -> ok.
send_prune({Id, Peer}) ->
    _ = Peer ! {?TAG_PRUNE, {Id, self()}},
    ok.
