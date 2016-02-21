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

-export([new/2, new/3]).
-export([broadcast/2]).
-export([system_broadcast/2]).
-export([handle_info/2]).
-export([neighbor_up/3]).
-export([neighbor_down/3]).
-export([get_member/1]).
-export([get_peers/1]).

-export_type([tree/0]).
-export_type([peer/0]).
-export_type([peer_type/0]).
-export_type([message/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(TAG_GOSSIP, 'GOSSIP').
-define(TAG_IHAVE,  'IHAVE').
-define(TAG_WEHAVE, 'WEHAVE').
-define(TAG_PRUNE,  'PRUNE').
-define(TAG_GRAFT,  'GRAFT').

-record(peer,
        {
          pid                      :: ppg:peer(),
          type = eager             :: peer_type(),
          nohave = gb_sets:empty() :: gb_sets:set(message_id())
        }).

-record(schedule,
        {
          timer = make_ref()          :: reference(),
          queue = ppg_pq:new() :: ppg_pq:heap({ppg_util:milliseconds(), event()})
        }).

-define(TREE, ?MODULE).
-record(?TREE,
        {
          %% Group Member
          member :: ppg:member(), % The destination process of broadcasted messages

          %% Peers and Messages Management
          connections              :: #{connection() => #peer{}},
          missing = #{}            :: #{message_id() => [Owner::connection()]},
          ihave = #{}              :: #{message_id() => message()},
          wehave = gb_sets:empty() :: gb_sets:set(message_id()),

          %% Internal Event Scheduler
          schedule = #schedule{} :: #schedule{},

          %% Protocol Parameters
          ihave_timeout           :: timeout(),
          wehave_retention_period :: timeout(),
          max_nohave_count        :: pos_integer()
        }).

-opaque tree() :: #?TREE{}.

-type peer_type() :: eager | lazy.

-type peer() :: #{
            pid        => ppg:peer(),
            connection => ppg_hyparview:connection(),
            type       => peer_type(),
            nohaves    => non_neg_integer()
           }.

-type event() :: {ihave_timeout, message_id(), connection()}
               | {wehave_expire, message_id()}.

-type message_id() :: reference().
-type message_type() :: 'APP' | 'SYS'.
-type message() :: {message_type(), ppg:message()}.

-type connection() :: ppg_hyparview:connection().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec default_options() -> [ppg:plumtree_option()].
default_options() ->
    [
     {ihave_timeout, 50},
     {wehave_retention_period, 5 * 1000},
     {max_nohave_count, 1000}
    ].

%% @equiv new(Member, Peers, default_options())
-spec new(ppg:member(), [{ppg:peer(), connection()}]) -> tree().
new(Member, Peers) ->
    new(Member, Peers, default_options()).

-spec new(ppg:member(), [{ppg:peer(), connection()}], [ppg:plumtree_option()]) -> tree().
new(Member, Peers, Options0) ->
    _ = is_list(Options0) orelse error(badarg, [Member, Peers, Options0]),
    Options1 = Options0 ++ default_options(),
    Get =
        fun (Key, Validate) ->
                Value = proplists:get_value(Key, Options1),
                _ = Validate(Value) orelse error(badarg, [Member, Peers, Options0]),
                Value
        end,

    #?TREE{
        member                  = Member,
        connections             = maps:from_list([{C, #peer{pid = P}}|| {P, C} <- Peers]),
        ihave_timeout           = Get(ihave_timeout, fun ppg_util:is_timeout/1),
        wehave_retention_period = Get(wehave_retention_period, fun ppg_util:is_timeout/1),
        max_nohave_count        = Get(max_nohave_count, fun ppg_util:is_pos_integer/1)
       }.

-spec neighbor_up(ppg:peer(), connection(), tree()) -> tree().
neighbor_up(PeerPid, Connection, Tree) ->
    Connections = maps:put(Connection, #peer{pid = PeerPid}, Tree#?TREE.connections),
    Tree#?TREE{connections = Connections}.

-spec neighbor_down(ppg:peer(), connection(), tree()) -> tree().
neighbor_down(_, Connection, Tree0) ->
    Connections = maps:remove(Connection, Tree0#?TREE.connections),
    Tree1 =
        maps:fold(fun (MsgId, _, Acc) -> move_to_wehave_if_satisfied(MsgId, Acc) end,
                  Tree0#?TREE{connections = Connections},
                  Tree0#?TREE.ihave),
    Missing =
        ppg_maps:filtermap(
          fun (_, IhaveList) ->
                  case lists:delete(Connection, IhaveList) of
                      []   -> false;
                      List -> {true, List}
                  end
          end,
          Tree1#?TREE.missing),
    Tree1#?TREE{missing = Missing}.

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
                  nohaves    => gb_sets:size(Peer#peer.nohave)
                } | Acc]
      end,
      [],
      Connections).

-spec handle_info(term(), tree()) -> {ok, tree()} | ignore.
handle_info({?TAG_GOSSIP, Connection, Arg}, Tree) -> {ok, handle_gossip(Arg, Connection, Tree)};
handle_info({?TAG_IHAVE,  Connection, Arg}, Tree) -> {ok, handle_ihave(Arg, Connection, Tree)};
handle_info({?TAG_WEHAVE, Connection, Arg}, Tree) -> {ok, handle_wehave(Arg, Connection, Tree)};
handle_info({?TAG_PRUNE,  Connection},      Tree) -> {ok, handle_prune(Connection, Tree)};
handle_info({?TAG_GRAFT,  Connection, Arg}, Tree) -> {ok, handle_graft(Arg, Connection, Tree)};
handle_info({?MODULE, schedule},            Tree) -> {ok, handle_schedule(ppg_util:now_ms(), Tree)};
handle_info(_Info, _Tree)                         -> ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_gossip({message_id(), message()}, connection(), tree()) -> tree().
handle_gossip({MsgId, Message}, Connection, Tree0) ->
    case maps:find(Connection, Tree0#?TREE.connections) of
        error      -> Tree0; % `Connection' has been disconnected
        {ok, Peer} ->
            case message_status(MsgId, Tree0) of
                wehave ->
                    %% 配送完了後に追加されたノードからのメッセージ
                    _ = Peer#peer.pid ! message_ihave(Connection, MsgId),
                    Tree0;
                ihave ->
                    %% 別の経路からすでに受信済み
                    _ = Peer#peer.pid ! message_ihave(Connection, MsgId),
                    _ = Peer#peer.pid ! message_prune(Connection),
                    Tree1 = become(lazy, Connection, Tree0),
                    remove_from_nohave(MsgId, Connection, Tree1);
                nohave ->
                    Tree1 = become(eager, Connection, Tree0),
                    push_and_deliver(MsgId, Message, Connection, Tree1)
            end
    end.

-spec handle_ihave(message_id(), connection(), tree()) -> tree().
handle_ihave(MsgId, Connection, Tree) ->
    case maps:find(Connection, Tree#?TREE.connections) of
        error      -> Tree; % `Connection' has been disconnected
        {ok, Peer} ->
            case message_status(MsgId, Tree) of
                ihave  -> remove_from_nohave(MsgId, Connection, Tree);
                nohave -> add_to_missing(MsgId, Connection, Tree);
                wehave ->
                    %% `MsgId'の隣人への配送完了後に、新規ノードが追加され、かつ、そのノードが別経路で`MsgId'を受け取った場合に、ここに来る可能性がある
                    %% (同じメッセージを相互にGOSSIPで送付した場合にも発生する可能性がある。グループ構築直後の重複経路が存在する際に発生しやすい)
                    _ = Peer#peer.pid ! message_wehave(Connection, MsgId),
                    Tree
            end
    end.

-spec handle_wehave(message_id(), connection(), tree()) -> tree().
handle_wehave(MsgId, Connection, Tree) ->
    case maps:is_key(Connection, Tree#?TREE.connections) of
        false -> Tree; % `Connection' has been disconnected
        true  -> remove_from_nohave(MsgId, Connection, Tree)
    end.

-spec handle_prune(connection(), tree()) -> tree().
handle_prune(Connection, Tree) ->
    case maps:is_key(Connection, Tree#?TREE.connections) of
        false -> Tree; % `Connection' has been disconnected
        true  -> become(lazy, Connection, Tree)
    end.

-spec handle_graft(message_id(), connection(), tree()) -> tree().
handle_graft(MsgId, Connection, Tree0) ->
    case maps:find(Connection, Tree0#?TREE.connections) of
        error      -> Tree0; % `Connection' has been disconnected
        {ok, Peer} ->
            Tree1 = become(eager, Connection, Tree0),
            _ = case maps:find(MsgId, Tree1#?TREE.ihave) of
                    error         -> Tree1; % The message retention period has expired
                    {ok, Message} -> Peer#peer.pid ! message_gossip(Connection, MsgId, Message)
                end,
            Tree1
    end.

-spec handle_schedule(ppg_util:milliseconds(), tree()) -> tree().
handle_schedule(Now, Tree0) ->
    Schedule = Tree0#?TREE.schedule,
    case ppg_pq:out(Schedule#schedule.queue) of
        empty                          -> Tree0;
        {{Time, _}, _} when Time > Now ->
            %% まだ指定時間に到達していない
            After = Time - Now + 4, % NOTE: `+4`によって、`handle_schedule/2`の実行回数は最大でも200回/秒に抑えられる
            Timer = erlang:send_after(After, self(), {?MODULE, schedule}),
            Tree0#?TREE{schedule = Schedule#schedule{timer = Timer}};
        {{_, {wehave_expire, MsgId}}, Queue} ->
            Wehave = gb_sets:delete(MsgId, Tree0#?TREE.wehave),
            handle_schedule(Now, Tree0#?TREE{schedule = Schedule#schedule{queue = Queue}, wehave = Wehave});
        {{_, {ihave_timeout, MsgId, Connection}}, Queue} ->
            Tree1 =
                case maps:is_key(MsgId,Tree0#?TREE.missing) andalso maps:find(Connection, Tree0#?TREE.connections) of
                    {ok, Peer} ->
                        _ = Peer#peer.pid ! message_graft(Connection, MsgId),
                        become(eager, Connection, Tree0);
                    _ ->
                        Tree0
                end,
            handle_schedule(Now, Tree1#?TREE{schedule = Schedule#schedule{queue = Queue}})
    end.

-spec schedule(timeout(), event(), #schedule{}) -> #schedule{}.
schedule(infinity, _,  Schedule) -> Schedule;
schedule(After, Event, Schedule) ->
    ExecutionTime = ppg_util:now_ms() + After,
    Queue = ppg_pq:in({ExecutionTime, Event}, Schedule#schedule.queue),
    case ppg_pq:peek(Schedule#schedule.queue) of
        {Next, _} when Next =< ExecutionTime ->
            Schedule#schedule{queue = Queue};
        _ ->
            Timer = ppg_util:cancel_and_send_after(Schedule#schedule.timer, After, self(), {?MODULE, schedule}),
            Schedule#schedule{queue = Queue, timer = Timer}
    end.

-spec message_status(message_id(), tree()) -> nohave | ihave | wehave.
message_status(MsgId, Tree) ->
    case gb_sets:is_member(MsgId, Tree#?TREE.wehave) of
        true  -> wehave;
        false ->
            case maps:is_key(MsgId, Tree#?TREE.ihave) of
                true  -> ihave;
                false -> nohave
            end
    end.

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
    ok = push(MsgId, Message, Sender, Tree),

    %% Delivers the message
    _ = case Message of
            {'APP', Data} -> Tree#?TREE.member ! Data;
            {'SYS', Data} -> self() ! Data
        end,
    move_to_ihave(MsgId, Message, Sender, Tree).

-spec push(message_id(), message(), connection()|undefined, tree()) -> ok.
push(MsgId, Message, Sender, Tree) ->
    ppg_maps:foreach(
      fun (C, #peer{pid = Pid, type = Type}) ->
              Pid ! case Type =:= eager andalso C =/= Sender of
                        true  -> message_gossip(C, MsgId, Message);
                        false -> message_ihave(C, MsgId)
                    end
      end,
      Tree#?TREE.connections).

-spec move_to_ihave(message_id(), message(), connection()|undefined, tree()) -> tree().
move_to_ihave(MsgId, Message, Sender, Tree) ->
    {Missing, Owners} =
        case maps:find(MsgId, Tree#?TREE.missing) of
            error      -> {Tree#?TREE.missing, [Sender]};
            {ok, List} -> {maps:remove(MsgId, Tree#?TREE.missing), [Sender | List]}
        end,
    Connections =
        maps:map(
          fun (C, P) ->
                  case lists:member(C, Owners) of
                      true  -> P;
                      false ->
                          _ = gb_sets:size(P#peer.nohave) >= Tree#?TREE.max_nohave_count andalso
                              begin
                                  %% メッセージ解放を阻害し続けるピアをkillする (TODO: disconnectの方が良いかもしれない)
                                  %% => このピアが動いているVM自体が何らかの理由でスローダウンしている可能性が高い
                                  exit(P#peer.pid, kill)
                              end,
                          P#peer{nohave = gb_sets:add(MsgId, P#peer.nohave)}
                  end
          end,
          Tree#?TREE.connections),
    Ihave = maps:put(MsgId, Message, Tree#?TREE.ihave),
    move_to_wehave_if_satisfied(MsgId, Tree#?TREE{connections = Connections, missing = Missing, ihave = Ihave}).

-spec move_to_wehave_if_satisfied(message_id(), tree()) -> tree().
move_to_wehave_if_satisfied(MsgId, Tree) ->
    DoesWehave =
        not ppg_maps:any(fun (_, #peer{nohave = Nohave}) -> gb_sets:is_member(MsgId, Nohave) end, Tree#?TREE.connections),
    case DoesWehave of
        false -> Tree;
        true  ->
            Ihave = maps:remove(MsgId, Tree#?TREE.ihave),
            Wehave = gb_sets:add(MsgId, Tree#?TREE.wehave),
            Schedule = schedule(Tree#?TREE.wehave_retention_period, {wehave_expire, MsgId}, Tree#?TREE.schedule),
            Tree#?TREE{ihave = Ihave, wehave = Wehave, schedule = Schedule}
    end.

-spec remove_from_nohave(message_id(), connection(), tree()) -> tree().
remove_from_nohave(MsgId, Connection, Tree) ->
    Peer = maps:get(Connection, Tree#?TREE.connections),
    case gb_sets:is_member(MsgId, Peer#peer.nohave) of
        false -> Tree;
        true  ->
            Nohave = gb_sets:delete(MsgId, Peer#peer.nohave),
            Connections = maps:put(Connection, Peer#peer{nohave = Nohave}, Tree#?TREE.connections),
            move_to_wehave_if_satisfied(MsgId, Tree#?TREE{connections = Connections})
    end.

-spec add_to_missing(message_id(), connection(), tree()) -> tree().
add_to_missing(MsgId, Connection, Tree) ->
    Owners = [Connection | maps:get(MsgId, Tree#?TREE.missing, [])],
    Missing = maps:put(MsgId, Owners, Tree#?TREE.missing),

    After = Tree#?TREE.ihave_timeout * length(Owners),
    Schedule = schedule(After, {ihave_timeout, MsgId, Connection}, Tree#?TREE.schedule),
    Tree#?TREE{missing = Missing, schedule = Schedule}.

-spec message_gossip(connection(), message_id(), message()) -> {?TAG_GOSSIP, connection(), {message_id(), message()}}.
message_gossip(Connection, MsgId, Message) ->
    {?TAG_GOSSIP, Connection, {MsgId, Message}}.

-spec message_ihave(connection(), message_id()) -> {?TAG_IHAVE, connection(), message_id()}.
message_ihave(Connection, MsgId) ->
    {?TAG_IHAVE, Connection, MsgId}.

-spec message_wehave(connection(), message_id()) -> {?TAG_WEHAVE, connection(), message_id()}.
message_wehave(Connection, MsgId) ->
    {?TAG_WEHAVE, Connection, MsgId}.

-spec message_graft(connection(), message_id()) -> {?TAG_GRAFT, connection(), message_id()}.
message_graft(Connection, MsgId) ->
    {?TAG_GRAFT, Connection, MsgId}.

-spec message_prune(connection()) -> {?TAG_PRUNE, connection()}.
message_prune(Connection) ->
    {?TAG_PRUNE, Connection}.
