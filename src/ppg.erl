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

-export([join/1]).
-export([leave/1]).
-export([broadcast/2]).

%% TODO: Move to ppg_debug module
%% -export([get_closest_pid/1]).
%% -export([get_members/1]).
%% -export([get_local_members/1]).
-export([get_graph/1]).

-export_type([name/0]).
-export_type([member/0]).

-export_type([communication_graph/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Types
%%----------------------------------------------------------------------------------------------------------------------
-type name() :: term().
-type member() :: pid().

-type communication_graph() :: [{Node::pid(), Member::pid(), [Edge::{eager|lazy, pid()}]}].

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
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

%% TODO: ppg_peer:peer()を返して、利用者側がlink or monitor or ignoreを選択可能にする
-spec join(name()) -> ok.
join(Group) ->
    case ppg_peer_sup:push_member(Group, self()) of
        {ok, _} -> ok;
        Other   -> error({badresult, Other}, [Group])
    end.

-spec leave(name()) -> ok.
leave(Group) ->
    ppg_peer_sup:pop_member(Group, self()).

-spec broadcast(name(), term()) -> ok.
broadcast(Group, Message) ->
    Peer = ppg_peer_sup:get_peer(Group, self()),
    ppg_peer:broadcast(Peer, Message).
