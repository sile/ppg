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

-export_type([tree/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(STATE, ?MODULE).

-record(?STATE,
        {
          destination :: pid(),
          eager_push_peers = [] :: [pid()],
          lazy_push_peers = [] :: [pid()],
          lazy_queue = [] :: list(),
          missing = #{} :: maps:map(),
          receives = #{} :: maps:map()
        }).

-opaque tree() :: #?STATE{}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new(pid(), [pid()]) -> tree().
new(Destination, Peers) ->
    _ = is_pid(Destination) orelse error(badarg, [Destination, Peers]),
    _ = is_list(Peers) andalso lists:all(fun is_pid/1, Peers) orelse error(badarg, [Destination, Peers]),
    #?STATE{
        destination = Destination,
        eager_push_peers = Peers
       }.

-spec broadcast(term(), tree()) -> tree().
broadcast(Message, Tree0) ->
    MsgId = make_ref(),
    Tree1 = eager_push(MsgId, Message, 0, self(), Tree0),
    Tree2 = lazy_push(MsgId, Message, 0, self(), Tree1),
    deliver(MsgId, Message, Tree2).

-spec handle_info(term(), tree()) -> {ok, tree()} | ignore | {error, term()}.
handle_info({'NEIGHBOR_UP', Peer},  Tree) ->
    handle_neighbor_up(Peer, Tree);
handle_info({'NEIGHBOR_DOWN', Peer}, Tree) ->
    handle_neighbor_down(Peer, Tree);
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

%-spec handle_neighbor_down(pid(), tree()) -> {ok, tree()}.
handle_neighbor_down(_Peer, _Tree) ->
    {error, unimplemented}.
