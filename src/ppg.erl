%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Plumtree based Process Group
-module(ppg).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([default_join_options/0]).

-export([create/1]).
-export([delete/1]).
-export([which_groups/0]).
-export([get_members/1]).
-export([get_local_members/1]).
-export([get_closest_member/1]).

-export([join/2, join/3]).
-export([leave/1]).
-export([broadcast/2]).

-export_type([name/0]).
-export_type([member/0]).
-export_type([peer/0]).
-export_type([message/0]).

-export_type([join_options/0, join_option/0]).
-export_type([plumtree_option/0]).
-export_type([hyparview_option/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Types
%%----------------------------------------------------------------------------------------------------------------------
-type name() :: term().
-type member() :: pid().
-type message() :: term().
-type peer() :: pid(). % TODO: 適切な名前に変える

-type join_options() :: [join_option()].

-type join_option() :: {plumtree, [plumtree_option()]}
                     | {hyparview, [hyparview_option()]}.

-type plumtree_option() :: {ihave_timeout, timeout()}
                         | {wehave_retention_period, timeout()}
                         | {max_nohave_count, pos_integer()}. % TODO: rename

-type hyparview_option() :: {active_view_size, pos_integer()}
                          | {passive_view_size, pos_integer()}
                          | {active_random_walk_length, pos_integer()}
                          | {passive_random_walk_length, pos_integer()}
                          | {shuffle_count, pos_integer()}
                          | {shuffle_interval, timeout()}
                          | {max_broadcast_delay, timeout()}
                          | {allowable_disconnection_period, timeout()}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec default_join_options() -> join_options().
default_join_options() ->
    [
     {plumtree, ppg_plumtree:default_options()},
     {hyparview, ppg_hyparview:default_options()}
    ].

%% NOTE: pg2とは異なりスコープはローカル (各ノードでの実行が必要)
-spec create(name()) -> ok.
create(Group) ->
    case ppg_group_sup:start_child(Group) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Other                         -> error({badresult, Other}, [Group])
    end.

-spec delete(name()) -> ok.
delete(Group) ->
    ppg_group_sup:stop_child(Group).

-spec which_groups() -> [name()].
which_groups() ->
    [Group || {Group, _} <- ppg_group_sup:which_children()].

%% TODO: 静止状態ではない場合には結果の正しさを保証しない旨を記述
-spec get_members(name()) -> {ok, [{member(), peer()}]} | {error, {no_such_group, name()}}.
get_members(Group) ->
    case get_closest_member(Group) of
        {error, Reason}  -> {error, Reason};
        {ok, {_, Peer0}} ->
            Members = [{Member, Peer1} || {Peer1, Member, _} <- ppg_peer:get_graph(Peer0, 5000)],
            {ok, Members}
    end.

%% TODO: ローカルにメンバーがいる or 静止状態ではない場合には結果の正しさを保証しない旨を記述
-spec get_closest_member(name()) -> {ok, {member(), peer()}} | {error, {no_such_group, name()}}.
get_closest_member(Group) ->
    case ppg_group_sup:find_child(Group) of
        error     -> {error, {no_such_group, Group}};
        {ok, Sup} ->
            case ppg_peer_sup:which_children(Sup) of
                []    -> todo;
                Peers ->
                    Peer = lists:nth(rand:uniform(length(Peers)), Peers),
                    {ok, {ppg_peer:get_member(Peer), Peer}}
            end
    end.

-spec get_local_members(name()) -> {ok, [{member(), peer()}]} | {error, {no_such_group, name()}}.
get_local_members(Group) ->
    case ppg_group_sup:find_child(Group) of
        error     -> {error, {no_such_group, Group}};
        {ok, Sup} ->
            Members = [{ppg_peer:get_member(Peer), Peer} || Peer <- ppg_peer_sup:which_children(Sup)],
            {ok, Members}
    end.

%% @equiv join(Group, Member, default_join_options())
-spec join(name(), ppg:member()) -> {ok, peer()} | {error, {no_such_group, name()}}.
join(Group, Member) ->
    join(Group, Member, default_join_options()).

%% NOTE: 必要であれば`Peer'に対してlink/monitorを行うこと
%%
%% `Peer'は{@link leave/1}や{@link broadcast/2}で使用する
-spec join(name(), ppg:member(), join_options()) -> {ok, Peer::peer()} | {error, {no_such_group, name()}}.
join(Group, Member, Options) ->
    _ = is_list(Options) orelse error(badarg, [Group, Member, Options]),
    case ppg_peer_sup:start_child(Group, Member, Options) of
        {ok, Pid}                   -> {ok, Pid};
        {error, {no_such_group, _}} -> {error, {no_such_group, Group}};
        Other                       -> error({badresult, Other}, [Group, Member, Options])
    end.

%% NOTE: `Peer'が生きていない場合には失敗する
-spec leave(peer()) -> ok.
leave(Peer) ->
    ppg_peer:stop(Peer).

%% NOTE: `Peer'が生きていない場合には失敗する
-spec broadcast(peer(), message()) -> ok.
broadcast(Peer, Message) ->
    ppg_peer:broadcast(Peer, Message).
