%% Copyright (c) 2016, Takeru Ohta <phjgt308@gmail.com>
%%
%% This software is released under the MIT License.
%% See the LICENSE file in the project root for full license information.
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
-export_type([channel/0]).
-export_type([message/0]).

-export_type([join_options/0, join_option/0]).
-export_type([plumtree_option/0]).
-export_type([hyparview_option/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-type name() :: term().
%% Group Name

-type member() :: pid().
%% A member process
%%
%% This process receives messages which are broadcasted to the belonging group

-type channel() :: pid().
%% A broadcast channel

-type message() :: term().
%% A broadcast message

-type join_options() :: [join_option()].

-type join_option() :: {plumtree, [plumtree_option()]}
                     | {hyparview, [hyparview_option()]}.

-type plumtree_option() :: {gossip_wait_timeout, timeout()}
                         | {ihave_retention_period, timeout()}
                         | {wehave_retention_period, timeout()}
                         | {ticktime, timeout()}.

-type hyparview_option() :: {active_view_size, pos_integer()}
                          | {passive_view_size, pos_integer()}
                          | {active_random_walk_length, pos_integer()}
                          | {passive_random_walk_length, pos_integer()}
                          | {shuffle_count, pos_integer()}
                          | {shuffle_interval, timeout()}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @doc Returns the default join options
-spec default_join_options() -> join_options().
default_join_options() ->
    [
     {plumtree, ppg_plumtree:default_options()},
     {hyparview, ppg_hyparview:default_options()}
    ].

%% @doc Creates a new group
%%
%% If the group already exists on the node, nothing happens.
%%
%% Unlike the pg2 module, every nodes which are interested in the group must call this function.
-spec create(name()) -> ok.
create(Group) ->
    case ppg_group_sup:start_child(Group) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Other                         -> error({badresult, Other}, [Group])
    end.

%% @doc Deletes the group from the local node
-spec delete(name()) -> ok.
delete(Group) ->
    ppg_group_sup:stop_child(Group).

%% @doc Returns the list of locally known groups
-spec which_groups() -> [name()].
which_groups() ->
    [Group || {Group, _} <- ppg_group_sup:which_children()].

%% @doc Returns all processes  in the group `Group'
%%
%% This function is provided for debugging purposes only.
-spec get_members(name()) -> {ok, [{member(), channel()}]} | {error, {no_such_group, name()}}.
get_members(Group) ->
    case get_closest_member(Group) of
        {error, Reason}  -> {error, Reason};
        {ok, {_, Peer0}} ->
            Members = [{Member, Peer1} || {Peer1, Member, _} <- ppg_peer:get_graph(Peer0, 5000)],
            {ok, Members}
    end.

%% @doc Returns a process on the local node, if such a process exist. Otherwise, it returns the contact service process.
-spec get_closest_member(name()) -> {ok, {member(), channel()}} | {error, Reason} when
      Reason :: {no_such_group, name()}
              | {no_reachable_member, name()}.
get_closest_member(Group) ->
    case ppg_group_sup:find_child(Group) of
        error     -> {error, {no_such_group, Group}};
        {ok, Sup} ->
            case ppg_peer_sup:which_children(Sup) of
                [] ->
                    case ppg_contact_service:find_peer(ppg_contact_service:new(Group)) of
                        error      -> {error, {no_reachable_member, Group}};
                        {ok, Peer} -> {ok, Peer}
                    end;
                Peers ->
                    Peer = lists:nth(rand:uniform(length(Peers)), Peers),
                    {ok, {ppg_peer:get_member(Peer), Peer}}
            end
    end.

%% @doc Returns all processes running on the local node in the group `Group'
-spec get_local_members(name()) -> {ok, [{member(), channel()}]} | {error, {no_such_group, name()}}.
get_local_members(Group) ->
    case ppg_group_sup:find_child(Group) of
        error     -> {error, {no_such_group, Group}};
        {ok, Sup} ->
            Members = [{ppg_peer:get_member(Peer), Peer} || Peer <- ppg_peer_sup:which_children(Sup)],
            {ok, Members}
    end.

%% @equiv join(Group, Member, default_join_options())
-spec join(name(), ppg:member()) -> {ok, channel()} | {error, {no_such_group, name()}}.
join(Group, Member) ->
    join(Group, Member, default_join_options()).

%% @doc Joins the process `Member' to the group `Group'
%%
%% After the joining, `Member' can broadcast messages via `Channel'.
%%
%% If you are interested in the disconnection of the channel,
%% you should apply {@link erlang:monitor/2} or {@link erlang:link/1} to the channel.
-spec join(name(), ppg:member(), join_options()) -> {ok, Channel::channel()} | {error, {no_such_group, name()}}.
join(Group, Member, Options) ->
    _ = is_list(Options) orelse error(badarg, [Group, Member, Options]),
    case ppg_peer_sup:start_child(Group, Member, Options) of
        {ok, Pid}                   -> {ok, Pid};
        {error, {no_such_group, _}} -> {error, {no_such_group, Group}};
        Other                       -> error({badresult, Other}, [Group, Member, Options])
    end.

%% @doc Leaves the group associated with `Channel'
-spec leave(channel()) -> ok.
leave(Channel) ->
    ppg_peer:stop(Channel).

%% @doc Broadcasts `Message' to the group associated with `Channel'
-spec broadcast(channel(), message()) -> ok.
broadcast(Channel, Message) ->
    ppg_peer:broadcast(Channel, Message).
