%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Plumtree based Process Group
-module(ppg).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([create/1]).
-export([delete/1]).
-export([which_groups/0]).

-export([join/1, join/2]).
-export([leave/1]).
-export([broadcast/2]).

%% TODO: Move to ppg_debug module
%% -export([get_closest_pid/1]).
%% -export([get_members/1]).
%% -export([get_local_members/1]).
-export([get_graph/1]).

-export([default_join_options/0]).

-export_type([name/0]).
-export_type([member/0]).
-export_type([message/0]).
-export_type([peer/0]).

-export_type([communication_graph/0]). % TODO: delete

-export_type([join_options/0, join_option/0]).
-export_type([plumtree_option/0]).
-export_type([hyparview_option/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Types
%%----------------------------------------------------------------------------------------------------------------------
-type name() :: term().
-type member() :: pid().
-type message() :: term().
-type peer() :: pid().

-type communication_graph() :: [{Node::pid(), Member::pid(), [Edge::{eager|lazy, pid()}]}].

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

%% -spec get_members(name()) -> [pid()] | {error, {no_such_group, name()}}.
%% get_members(Group) ->
%%     case get_graph(Group) of
%%         {error, Reason}          -> {error, Reason};
%%         Graph                    -> [Member || {_, Member, _} <- Graph]
%%     end.

-spec get_graph(name()) -> communication_graph() | {error, {no_such_group, name()}}.
get_graph(Group) ->
    Peer = ppg_peer_sup:get_peer(Group, self()),
    ppg_peer:get_graph(Peer, 1000).

%% -spec get_local_members(name()) -> [pid()] | {error, {no_such_group, name()}}.
%% get_local_members(Group) ->
%%     case pg2:get_members(?PG2_NAME(Group)) of
%%         {error, {no_such_group, _}} -> {error, {no_such_group, Group}};
%%         _                           -> [Member || {_, Member} <- ppg_peer_sup:which_children(Group)]
%%     end.

%% -spec get_closest_pid(name()) -> pid() | {error, Reason} when
%%       Reason :: {no_process, name()} | {no_such_group, name()}.
%% get_closest_pid(Group) ->
%%     case get_closest_peer(Group) of
%%         {error, Reason} -> {error, Reason};
%%         Peer            -> ppg_peer:get_destination(Peer)
%%     end.

%% @equiv join(Group, default_join_options())
-spec join(name()) -> {ok, peer()}.
join(Group) ->
    join(Group, default_join_options()).

-spec join(name(), join_options()) -> {ok, peer()}.
join(Group, Options) ->
    _ = is_list(Options) orelse error(badarg, [Group, Options]),
    case ppg_peer_sup:push_member(Group, self(), Options) of
        {ok, Pid} ->
            _ = link(Pid), % TODO:
            {ok, Pid};
        Other     -> error({badresult, Other}, [Group])
    end.

-spec leave(name()) -> ok.
leave(Group) ->
    ppg_peer_sup:pop_member(Group, self()).

-spec broadcast(name(), message()) -> ok.
broadcast(Group, Message) ->
    Peer = ppg_peer_sup:get_peer(Group, self()),
    ppg_peer:broadcast(Peer, Message).
