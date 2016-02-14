%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Plumtree Peer Process
%% @private
-module(ppg_plumtree).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/2]).
-export([broadcast/2]).
-export([handle_info/2]).
-export([get_entry/1]).
-export([notify_neighbor_up/1]).
-export([notify_neighbor_down/1]).

-export_type([tree/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(TAG_GOSSIP, 'GOSSIP').
-define(TAG_IHAVE, 'IHAVE').
-define(TAG_PRUNE, 'PRUNE').
-define(TAG_GRAFT, 'GRAFT').

-define(STATE, ?MODULE). % TODO: TREE

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

-record(?STATE,
        {
          member                            :: ppg:member(),
          peers = #{}                       :: #{ppg_peer:peer() => #peer{}},
          missing = #{}                     :: #{msg_id() => IHave::[ppg_peer:peer()]},
          received = #{}                    :: #{msg_id() => message()},
          delivered = gb_sets:empty()       :: gb_sets:set(msg_id()),
          schedule = #schedule{}            :: #schedule{},

          max_nohave_count = 100 :: pos_integer(),
          done_timeout = 1000 :: milliseconds(), % XXX: name
          ihave_timeout = 100 :: timeout()
        }).

-opaque tree() :: #?STATE{}.

-type msg_id() :: reference().
-type message() :: term().

-type milliseconds() :: non_neg_integer().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new(ppg:member(), [ppg_peer:peer()]) -> tree().
new(Member, Peers) ->
    #?STATE{
        member = Member,
        peers = maps:from_list([{P, #peer{}} || P <- Peers])
       }.

-spec broadcast(term(), tree()) -> tree().
broadcast(Message, Tree0) ->
    Id = make_ref(),
    Tree1 = push(Id, Message, self(), Tree0),
%%    ok = lazy_push(Id, self(), Tree1),
    Tree2 = received(Id, Message, self(), Tree1),
    deliver(Id, Message, Tree2).

-spec notify_neighbor_up(ppg_peer:peer()) -> ok.
notify_neighbor_up(Peer) ->
    _ = self() ! {'NEIGHBOR_UP', Peer},
    ok.

-spec notify_neighbor_down(ppg_peer:peer()) -> ok.
notify_neighbor_down(Peer) ->
    _ = self() ! {'NEIGHBOR_DOWN', Peer},
    ok.

-spec get_entry(tree()) -> Todo::term(). % TODO: Return message history size
get_entry(Tree) ->
    {self(), Tree#?STATE.member, Tree#?STATE.peers}.

-spec handle_info(term(), tree()) -> {ok, tree()} | ignore.
handle_info({?TAG_GOSSIP, Arg}, Tree) -> handle_gossip(Arg, Tree);
handle_info({?TAG_IHAVE, Arg}, Tree) -> handle_ihave(Arg, Tree);
handle_info({?TAG_PRUNE, Arg}, Tree) -> handle_prune(Arg, Tree);
handle_info({?TAG_GRAFT, Arg}, Tree) -> handle_graft(Arg, Tree);
handle_info({'NEIGHBOR_UP', Peer},  Tree) ->
    handle_neighbor_up(Peer, Tree);
handle_info({'NEIGHBOR_DOWN', Peer}, Tree) ->
    handle_neighbor_down(Peer, Tree);
handle_info({?MODULE, schedule}, Tree) ->
    handle_schedule(now_ms(), Tree);
handle_info(_Info, _Tree) ->
    ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_neighbor_up(ppg_peer:peer(), tree()) -> {ok, tree()}.
handle_neighbor_up(Peer, Tree) ->
    _ = maps:is_key(Peer, Tree#?STATE.peers) andalso error(duplicated_peer, [Peer, Tree]),
    Nohave =
        maps:fold(fun (Id, _, Acc) ->
                          _ = Peer ! message_ihave(Id),
                          gb_sets:add(Id, Acc)
                  end,
                  gb_sets:empty(),
                  Tree#?STATE.received),
    Peers = maps:put(Peer, #peer{nohave = Nohave}, Tree#?STATE.peers),
    {ok, Tree#?STATE{peers = Peers}}.

%% TODO:
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

-spec handle_neighbor_down(ppg_peer:peer(), tree()) -> {ok, tree()}.
handle_neighbor_down(Peer, Tree) ->
    _ = maps:is_key(Peer, Tree#?STATE.peers) orelse error(unknown_peer, [Peer, Tree]),
    Peers = maps:remove(Peer, Tree#?STATE.peers),

    {Received, Delivered, Schedule} =
        maps:fold(
          fun (Id, Message, {AccR, AccD, AccS}) ->
                  case is_delivered(Id, Peers) of
                      true  -> {AccR, gb_sets:add(Id, AccD), schedule(Tree#?STATE.done_timeout, {done, Id}, AccS)};
                      false -> {maps:put(Id, Message, AccR), AccD, AccS}
                  end
          end,
          {#{}, Tree#?STATE.delivered, Tree#?STATE.schedule},
          Tree#?STATE.received),
    Missing =
        maps:fold(
          fun (Id, IhaveList, Acc) ->
                  case lists:delete(Peer, IhaveList) of
                      []   -> Acc;
                      List -> maps:put(Id, List, Acc)
                  end
          end,
          #{},
          Tree#?STATE.missing),
    {ok, Tree#?STATE{peers = Peers, received = Received, missing = Missing,
                     delivered = Delivered, schedule = Schedule}}.

%% TODO:
-spec message_type(msg_id(), tree()) -> new | missing | received | delivered.
message_type(Id, Tree) ->
    case gb_sets:is_member(Id, Tree#?STATE.delivered) of
        true  -> delivered;
        false ->
            case maps:is_key(Id, Tree#?STATE.missing) of
                true  -> missing;
                false ->
                    case maps:is_key(Id, Tree#?STATE.received) of
                        true  -> received;
                        false -> new
                    end
            end
    end.

-spec become_lazy(ppg_peer:peer(), tree()) -> tree().
become_lazy(Peer, Tree) ->
    #{Peer := State} = Tree#?STATE.peers,
    case State#peer.type =:= lazy of
        true  -> Tree;
        false ->
            Peers = maps:put(Peer, State#peer{type = lazy}, Tree#?STATE.peers),
            Tree#?STATE{peers = Peers}
    end.

-spec become_eager(ppg_peer:peer(), tree()) -> tree().
become_eager(Peer, Tree) ->
    #{Peer := State} = Tree#?STATE.peers,
    case State#peer.type =:= eager of
        true  -> Tree;
        false ->
            Peers = maps:put(Peer, State#peer{type = eager}, Tree#?STATE.peers),
            Tree#?STATE{peers = Peers}
    end.

%% TODO:
-spec remove_from_nohave(msg_id(), ppg_peer:peer(), tree()) -> tree().
remove_from_nohave(Id, Peer, Tree) ->
    #{Peer := State} = Tree#?STATE.peers,
    case gb_sets:is_member(Id, State#peer.nohave) of
        false -> Tree;
        true  ->
            Peers = maps:put(Peer, State#peer{nohave = gb_sets:delete(Id, State#peer.nohave)}, Tree#?STATE.peers),
            case is_delivered(Id, Peers) of
                false ->
                    Tree#?STATE{peers = Peers};
                true ->
                    io:format("REMOVE [~p] id=~p\n", [self(), Id]),
                    Delivered = gb_sets:add(Id, Tree#?STATE.delivered),
                    Schedule = schedule(Tree#?STATE.done_timeout, {done, Id}, Tree#?STATE.schedule),
                    Received = maps:remove(Id, Tree#?STATE.received),
                    Tree#?STATE{peers = Peers, received = Received, delivered = Delivered, schedule = Schedule}
            end
    end.

-spec handle_gossip({msg_id(), message(), ppg_peer:peer()}, tree()) -> {ok, tree()}.
handle_gossip({Id, Message, Sender}, Tree0) ->
    case maps:is_key(Sender, Tree0#?STATE.peers) of
        false ->
            %% TODO: disconnect
            {ok, Tree0};
        true ->
            case message_type(Id, Tree0) of
                delivered ->
                    %% 配送完了後に追加されたノードからのメッセージ
                    _ = Sender ! message_ihave(Id),
                    {ok, Tree0};
                received ->
                    %% 別の経路からすでに受信済み
                    _ = Sender ! message_ihave(Id),
                    _ = Sender ! message_prune(),
                    Tree1 = become_lazy(Sender, Tree0),
                    Tree2 = remove_from_nohave(Id, Sender, Tree1),
                    {ok, Tree2};
                _ -> % missing | new
                    %% _ = Sender ! message_ihave(Id), % NOTE: メッセージ解放を早く行えるようにするためにackを返す
                    Tree1 = deliver(Id, Message, Tree0),
                    Tree2 = push(Id, Message, Sender, Tree1),
                    Tree3 = become_eager(Sender, Tree2),
                    Tree4 = received(Id, Message, Sender, Tree3),
                    {ok, Tree4}
            end
    end.

-spec received(msg_id(), message(), ppg_peer:peer(), tree()) -> tree().
received(Id, Message, Sender, Tree) ->
    {Missing, IhaveList} =
        case maps:find(Id, Tree#?STATE.missing) of
            error      -> {Tree#?STATE.missing, [Sender] -- [self()]}; % XXX:
            {ok, List} -> {maps:remove(Id, Tree#?STATE.missing), [Sender | List] -- [self()]}
        end,
    case maps:size(Tree#?STATE.peers) =:= length(IhaveList) of
        true ->
            Delivered = gb_sets:add(Id, Tree#?STATE.delivered),
            Schedule = schedule(Tree#?STATE.done_timeout, {done, Id}, Tree#?STATE.schedule),
            Tree#?STATE{missing = Missing, delivered = Delivered, schedule = Schedule};
        false ->
            Peers =
                maps:map(
                  fun (P, S) ->
                          case lists:member(P, IhaveList) of
                              true  -> S;
                              false -> S#peer{nohave = gb_sets:add(Id, S#peer.nohave)}
                          end
                  end,
                  Tree#?STATE.peers),
            Received = maps:put(Id, Message, Tree#?STATE.received),
            Tree#?STATE{peers = Peers, missing = Missing, received = Received}
    end.

-spec handle_ihave({msg_id(), ppg_peer:peer()}, tree()) -> {ok, tree()}.
handle_ihave({Id, Sender}, Tree) ->
    case maps:is_key(Sender, Tree#?STATE.peers) of
        false -> {ok, Tree}; %% TODO: disconnect
        true  ->
            case message_type(Id, Tree) of
                delivered -> {ok, Tree}; % TODO: reply ihave(?) => graft時にnohaveに含まれないようになっているなら不要
                received  -> {ok, remove_from_nohave(Id, Sender, Tree)};
                _         -> {ok, missing(Id, Sender, Tree)}
            end
    end.

-spec missing(msg_id(), ppg_peer:peer(), tree()) -> tree().
missing(Id, Sender, Tree) ->
    {Missing, Schedule} =
        case maps:find(Id, Tree#?STATE.missing) of
            error      -> {maps:put(Id, [Sender], Tree#?STATE.missing),
                           schedule(Tree#?STATE.ihave_timeout, {ihave, Id}, Tree#?STATE.schedule)};
            {ok, List} -> {maps:put(Id, List ++ [Sender], Tree#?STATE.missing), Tree#?STATE.schedule}
        end,
    Tree#?STATE{missing = Missing, schedule = Schedule}.

-spec handle_schedule(milliseconds(), tree()) -> {ok, tree()}.
handle_schedule(Now, Tree) ->
    Schedule = Tree#?STATE.schedule,
    case pairing_heaps:out(Schedule#schedule.queue) of
        empty                          -> {ok, Tree};
        {{Time, _}, _} when Time > Now ->
            _ = erlang:cancel_timer(Schedule#schedule.timer), % 念のため
            _ = receive {?MODULE, schedule} -> ok after 0 -> ok end,
            Timer = erlang:send_after(Time - Now + 1, self(), {?MODULE, schedule}),
            {ok, Tree#?STATE{schedule = Schedule#schedule{timer = Timer}}};
        {{_, {done, Id}}, Queue} ->
            {ok, Tree1} = handle_done(Id, Tree),
            handle_schedule(Now, Tree1#?STATE{schedule = Schedule#schedule{queue = Queue}});
        {{_, {ihave, Id}}, Queue} ->
            {ok, Tree1} = handle_ihave_timeout(Id, Tree),
            handle_schedule(Now, Tree1#?STATE{schedule = Schedule#schedule{queue = Queue}})
    end.

-spec handle_done(msg_id(), tree()) -> {ok, tree()}.
handle_done(Id, Tree) ->
    Delivered = gb_sets:delete(Id, Tree#?STATE.delivered),
    {ok, Tree#?STATE{delivered = Delivered}}.

-spec handle_ihave_timeout(msg_id(), tree()) -> {ok, tree()}.
handle_ihave_timeout(Id, Tree0) ->
    case maps:find(Id, Tree0#?STATE.missing) of
        error                    -> {ok, Tree0};
        {ok, [Peer | IhaveList]} ->
            _ = Peer ! message_graft(Id),
            Tree1 = become_eager(Peer, Tree0),
            case IhaveList of
                [] ->
                    Missing = maps:remove(Id, Tree1#?STATE.missing),
                    {ok, Tree1#?STATE{missing = Missing}};
                _  ->
                    Schedule = schedule(Tree1#?STATE.ihave_timeout, {ihave, Id}, Tree1#?STATE.schedule),
                    Missing = maps:put(Id, IhaveList, Tree1#?STATE.missing),
                    {ok, Tree1#?STATE{missing = Missing, schedule = Schedule}}
            end
    end.

-spec handle_graft({msg_id(), ppg_peer:peer()}, tree()) -> {ok, tree()}.
handle_graft({Id, Sender}, Tree0) ->
    case maps:is_key(Sender, Tree0#?STATE.peers) of
        false -> {ok, Tree0}; %% TODO: disconnect
        true  ->
            Tree1 = become_eager(Sender, Tree0),
            case maps:find(Id, Tree1#?STATE.received) of
                error         -> {ok, Tree1}; % maybe timed out
                {ok, Message} ->
                    _ = Sender ! message_gossip(Id, Message),
                    {ok, Tree1}
            end
    end.

-spec handle_prune(ppg_peer:peer(), tree()) -> {ok, tree()}.
handle_prune(Sender, Tree) ->
    case maps:is_key(Sender, Tree#?STATE.peers) of
        false -> {ok, Tree}; % TODO: disconnect
        true  -> {ok, become_lazy(Sender, Tree)}
    end.

-spec push(msg_id(), term(), pid(), tree()) -> tree().
push(Id, Message, Sender, Tree) ->
    _ = maps:fold(
          fun (P, #peer{type = eager}, _) when P =/= Sender ->
                  P ! message_gossip(Id, Message);
              (P, _, _) ->
                  P ! message_ihave(Id)
          end,
          ok,
          Tree#?STATE.peers),
    Tree.

-spec deliver(msg_id(), term(), tree()) -> tree().
deliver(_MsgId, Message, Tree) ->
    %% TODO:
    _ = case Message of
            {'SYSTEM', get_graph, {Ref, From}} -> % TODO:
                From ! {Ref, get_entry(Tree)};
            {'INTERNAL', X} -> % TODO:
                self() ! X;
            _ ->
                Tree#?STATE.member ! Message
        end,
    Tree.

-spec now_ms() -> milliseconds().
now_ms() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000 * 1000 * 1000) + (Sec * 1000) + (Micro div 1000).

-spec message_gossip(msg_id(), term()) -> {?TAG_GOSSIP, {msg_id(), term(), ppg_peer:peer()}}.
message_gossip(MsgId, Message) -> {?TAG_GOSSIP, {MsgId, Message, self()}}.

-spec message_ihave(msg_id()) -> {?TAG_IHAVE, {msg_id(), ppg_peer:peer()}}.
message_ihave(MsgId) -> {?TAG_IHAVE, {MsgId, self()}}.

-spec message_prune() -> {?TAG_PRUNE, ppg_peer:peer()}.
message_prune() -> {?TAG_PRUNE, self()}.

-spec message_graft(msg_id()) -> {?TAG_GRAFT, {msg_id(), ppg_peer:peer()}}.
message_graft(MsgId) -> {?TAG_GRAFT, {MsgId, self()}}.
