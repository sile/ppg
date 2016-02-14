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
-define(STATE, ?MODULE).

-record(?STATE,
        {
          member :: ppg:member(),
          eager_push_peers = [] :: [ppg_peer:peer()],
          lazy_push_peers = [] :: [ppg_peer:peer()],
          lazy_queue = [] :: list(),
          missing = #{} :: #{},
          receives = #{} :: #{}
        }).

-opaque tree() :: #?STATE{}.

-type msg_id() :: reference().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new(ppg:member(), [ppg_peer:peer()]) -> tree().
new(Member, Peers) ->
    #?STATE{
        member = Member,
        eager_push_peers = Peers
       }.

-spec broadcast(term(), tree()) -> tree().
broadcast(Message, Tree0) ->
    MsgId = make_ref(),
    Tree1 = eager_push(MsgId, Message, self(), Tree0),
    Tree2 = lazy_push(MsgId, Message, self(), Tree1),
    deliver(MsgId, Message, Tree2).

-spec notify_neighbor_up(ppg_peer:peer()) -> ok.
notify_neighbor_up(Peer) ->
    _ = self() ! {'NEIGHBOR_UP', Peer},
    ok.

-spec notify_neighbor_down(ppg_peer:peer()) -> ok.
notify_neighbor_down(Peer) ->
    _ = self() ! {'NEIGHBOR_DOWN', Peer},
    ok.

-spec get_entry(tree()) -> Todo::term().
get_entry(Tree) ->
    Edges =
        [{eager, P} || P <- Tree#?STATE.eager_push_peers] ++
        [{lazy, P} || P <- Tree#?STATE.lazy_push_peers],
    {self(), Tree#?STATE.member, Edges}.

-spec handle_info(term(), tree()) -> {ok, tree()} | ignore.
handle_info({'GOSSIP', Arg}, Tree) ->
    handle_gossip(Arg, Tree);
handle_info({'IHAVE', Arg}, Tree) ->
    handle_ihave(Arg, Tree);
handle_info({'PRUNE', Arg}, Tree) ->
    handle_prune(Arg, Tree);
handle_info({'NEIGHBOR_UP', Peer},  Tree) ->
    handle_neighbor_up(Peer, Tree);
handle_info({'NEIGHBOR_DOWN', Peer}, Tree) ->
    handle_neighbor_down(Peer, Tree);
handle_info({ihave_timeout, Arg}, Tree) ->
    handle_ihave_timeout(Arg, Tree);
handle_info({'GRAFT', Arg}, Tree) ->
    handle_graft(Arg, Tree);
handle_info(_Info, _Tree) ->
    ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec handle_neighbor_up(pid(), tree()) -> {ok, tree()}.
handle_neighbor_up(Peer, Tree) ->
    case lists:member(Peer, Tree#?STATE.eager_push_peers ++ Tree#?STATE.lazy_push_peers) of
        true  -> {ok, Tree};
        false ->
            EagerPeers = [Peer | Tree#?STATE.eager_push_peers],
            {ok, Tree#?STATE{eager_push_peers = EagerPeers}}
    end.

-spec handle_neighbor_down(pid(), tree()) -> {ok, tree()}.
handle_neighbor_down(Peer, Tree0) ->
    Tree1 = remove_eager(Peer, remove_lazy(Peer, Tree0)),
    Missing =
        maps:filter(
          fun (_, {Timer, List}) ->
                  _ = List =:= [] andalso erlang:cancel_timer(Timer),
                  List =/= []
          end,
          maps:map(
            fun (_, {Timer, List}) ->
                    {Timer, lists:filter(fun ({Pid, _}) -> Pid =/= Peer end, List)}
            end,
            Tree1#?STATE.missing)),
    {ok, Tree1#?STATE{missing = Missing}}.

-spec handle_gossip({msg_id(), term(), pid()}, tree()) -> {ok, tree()}.
handle_gossip({MsgId, Message, Sender}, Tree0) ->
    case Tree0#?STATE.receives of
        #{MsgId := _} ->
            Tree1 = add_lazy(Sender, remove_eager(Sender, Tree0)),
            _ = Sender ! {'PRUNE', self()},
            {ok, Tree1};
        _ ->
            Tree1 = deliver(MsgId, Message, Tree0),
            Tree2 =
                case Tree1#?STATE.missing of
                    #{MsgId := {Timer, _}} ->
                        _ = erlang:cancel_timer(Timer),
                        Missing = maps:remove(MsgId, Tree1#?STATE.missing),
                        Tree1#?STATE{missing = Missing};
                    _ ->
                        Tree1
                end,
            Tree3 = eager_push(MsgId, Message, Sender, Tree2),
            Tree4 = lazy_push(MsgId, Message, Sender, Tree3),
            Tree5 = add_eager(Sender, remove_lazy(Sender, Tree4)),
            {ok, Tree5}
    end.

-spec handle_ihave({msg_id(), pid()}, tree()) -> {ok, tree()}.
handle_ihave({MsgId, Sender}, Tree) ->
    case Tree#?STATE.receives of
        #{MsgId := _} -> {ok, Tree};
        _             ->
            case Tree#?STATE.missing of
                #{MsgId := {Timer, List}} ->
                    Missing = maps:put(MsgId, {Timer, [Sender | List]}, Tree#?STATE.missing),
                    {ok, Tree#?STATE{missing = Missing}};
                _ ->
                    IhaveTimeout = 1000, % TODO
                    Timer = erlang:send_after(IhaveTimeout, self(), {ihave_timeout, MsgId}),
                    Missing = maps:put(MsgId, {Timer, [Sender]}, Tree#?STATE.missing),
                    {ok, Tree#?STATE{missing = Missing}}
            end
    end.

-spec handle_ihave_timeout(msg_id(), tree()) -> {ok, tree()}.
handle_ihave_timeout(MsgId, Tree0) ->
    case Tree0#?STATE.missing of
        #{MsgId := {_, [Pid | IhaveList]}} ->
            IhaveTimeout = 500,  % TODO:
            Timer = erlang:send_after(IhaveTimeout, self(), {ihave_timeout, MsgId}),
            Missing = maps:put(MsgId, {Timer, IhaveList}, Tree0#?STATE.missing),
            Tree1 = add_eager(Pid, remove_lazy(Pid, Tree0)),
            _ = Pid ! {'GRAFT', {MsgId, self()}},
            Tree2 = Tree1#?STATE{missing = Missing},
            {ok, Tree2};
        _ ->
            {ok, Tree0} % The message is already delivered in another path
    end.

-spec handle_graft({msg_id(), pid()}, tree()) -> {ok, tree()}.
handle_graft({MsgId, Sender}, Tree0) ->
    %% TODO: lazyにいない場合には拒否した方が良いかもしれない (HyParViewのレイヤーですでに切断されている可能性があるので)
    Tree1 = add_eager(Sender, remove_lazy(Sender, Tree0)),
    case Tree0#?STATE.receives of
        #{MsgId := Message} ->
            _ = Sender ! {'GOSSIP', {MsgId, Message, self()}},
            {ok, Tree1};
        _ ->
            {ok, Tree1}
    end.

-spec handle_prune(pid(), tree()) -> {ok, tree()}.
handle_prune(Sender, Tree0) ->
    Tree1 = add_lazy(Sender, remove_eager(Sender, Tree0)),
    {ok, Tree1}.

-spec eager_push(msg_id(), term(), pid(), tree()) -> tree().
eager_push(MsgId, Message, Sender, Tree) ->
    ok = lists:foreach(
           fun (Pid) ->
                   Pid =/= Sender andalso
                       begin
                           %% for debug
                           %% timer:sleep(rand:uniform(500)),
                           Pid ! {'GOSSIP', {MsgId, Message, self()}}
                       end
           end,
           Tree#?STATE.eager_push_peers),
    Tree.

-spec lazy_push(msg_id(), term(), pid(), tree()) -> tree().
lazy_push(MsgId, Message, Sender, Tree) ->
    Queue =
        lists:foldl(
          fun (Pid, Acc) when Pid =:= Sender ->
                  Acc;
              (Pid, Acc) ->
                  [{'IHAVE', Pid, MsgId, Message, self()} | Acc]
          end,
          Tree#?STATE.lazy_queue,
          Tree#?STATE.lazy_push_peers),
    dispatch(Tree#?STATE{lazy_queue = Queue}).

-spec dispatch(tree()) -> tree().
dispatch(Tree) ->
    %% TODO: piggy back on an application message
    ok = lists:foreach(
           fun ({'IHAVE', Pid, MsgId, _, Sender}) ->
                   Pid ! {'IHAVE', {MsgId, Sender}}
           end,
           Tree#?STATE.lazy_queue),
    Tree#?STATE{lazy_queue = []}.

-spec deliver(msg_id(), term(), tree()) -> tree().
deliver(MsgId, Message, Tree) ->
    _ = case Message of
            {'SYSTEM', get_graph, {Ref, From}} -> % TODO:
                From ! {Ref, get_entry(Tree)};
            {'INTERNAL', X} -> % TODO:
                self() ! X;
            _ ->
                Tree#?STATE.member ! Message
        end,
    Receives = maps:put(MsgId, Message, Tree#?STATE.receives),
    Tree#?STATE{receives = Receives}.

-spec add_eager(pid(), tree()) -> tree().
add_eager(Peer, Tree) ->
    case lists:member(Peer, Tree#?STATE.eager_push_peers) of
        true  -> Tree;
        false -> Tree#?STATE{eager_push_peers = [Peer | Tree#?STATE.eager_push_peers]}
    end.

-spec remove_eager(pid(), tree()) -> tree().
remove_eager(Peer, Tree) ->
    Tree#?STATE{eager_push_peers = lists:delete(Peer, Tree#?STATE.eager_push_peers)}.

-spec add_lazy(pid(), tree()) -> tree().
add_lazy(Peer, Tree) ->
    case lists:member(Peer, Tree#?STATE.lazy_push_peers) of
        true  -> Tree;
        false -> Tree#?STATE{lazy_push_peers = [Peer | Tree#?STATE.lazy_push_peers]}
    end.

-spec remove_lazy(pid(), tree()) -> tree().
remove_lazy(Peer, Tree) ->
    Tree#?STATE{lazy_push_peers = lists:delete(Peer, Tree#?STATE.lazy_push_peers)}.
