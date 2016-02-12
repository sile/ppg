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
-export([join/2, join/3]).
-export([leave/2]).
-export([get_closest_pid/1]).
-export([get_members/1]).
-export([get_local_members/1]).
-export([get_graph/1]).
-export([broadcast/2]).

-export_type([name/0]).
-export_type([communication_graph/0]).
-export_type([join_option/0, join_options/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(PG2_NAME(Name), {ppg_contact_peer, Name}).

-type name() :: term().

-type communication_graph() :: [{Node::pid(), Member::pid(), [Edge::{eager|lazy, pid()}]}].

-type join_options() :: [join_option()].
-type join_option() :: {contact_process_count, pos_integer()}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec default_join_options() -> join_options().
default_join_options() ->
    [
     %% {contact_process_count, 3} % TODO:
     {contact_process_count, 1}
    ].

-spec create(name()) -> ok.
create(Group) ->
    pg2:create(?PG2_NAME(Group)).

-spec delete(name()) -> ok.
delete(Group) ->
    case pg2:get_members(?PG2_NAME(Group)) of
        {error, {no_such_group, _}} -> ok;
        Peers                       ->
            ok = lists:foreach(fun ppg_peer:stop_all/1, Peers),
            pg2:delete(?PG2_NAME(Group))
    end.

-spec which_groups() -> [name()].
which_groups() ->
    [Group || ?PG2_NAME(Group) <- pg2:which_groups()].

-spec get_members(name()) -> [pid()] | {error, {no_such_group, name()}}.
get_members(Group) ->
    case get_graph(Group) of
        {error, Reason}          -> {error, Reason};
        Graph                    -> [Member || {_, Member, _} <- Graph]
    end.

-spec get_graph(name()) -> communication_graph() | {error, {no_such_group, name()}}.
get_graph(Group) ->
    case get_closest_peer(Group) of
        {error, {no_process, _}} -> [];
        {error, Reason}          -> {error, Reason};
        Peer                     -> ppg_peer:get_graph(Peer, 1000)
    end.

-spec get_local_members(name()) -> [pid()] | {error, {no_such_group, name()}}.
get_local_members(Group) ->
    case pg2:get_members(?PG2_NAME(Group)) of
        {error, {no_such_group, _}} -> {error, {no_such_group, Group}};
        _                           -> [Member || {_, Member} <- ppg_peer_sup:which_children(Group)]
    end.

-spec get_closest_pid(name()) -> pid() | {error, Reason} when
      Reason :: {no_process, name()} | {no_such_group, name()}.
get_closest_pid(Group) ->
    case get_closest_peer(Group) of
        {error, Reason} -> {error, Reason};
        Peer            -> ppg_peer:get_destination(Peer)
    end.

-spec join(name(), pid()) -> ok | {error, {no_such_group, name()}}.
join(Group, Member) ->
    join(Group, Member, []).

-spec join(name(), pid(), join_options()) -> ok | {error, {no_such_group, name()}}.
join(Group, Member, Options) ->
    Args = [Group, Member, Options],
    _ = ppg_util:is_local_pid(Member) orelse error(badarg, Args),
    _ = is_list(Options) orelse error(badarg, Args),
    _ = ppg_peer:is_valid_options(Options) orelse error(badarg, Args),

    case ppg_peer_sup:start_child(Group, Member, Options ++ default_join_options()) of
        {ok, _}         -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec leave(name(), pid()) -> ok | {error, {no_such_group, name()}}.
leave(Group, Member) ->
    Args = [Group, Member],
    _ = ppg_util:is_local_pid(Member) orelse error(badarg, Args),

    case pg2:get_members(?PG2_NAME(Group)) of
        {error, {no_such_group, _}} -> {error, {no_such_group, Group}};
        _                           ->
            case ppg_peer_sup:select_children_by_destination(Group, Member) of
                []         -> ok;
                [Peer | _] -> ppg_peer:stop(Peer)
            end
    end.

-spec broadcast(name(), term()) -> ok | {error, {no_such_group, name()}}.
broadcast(Group, Message) ->
    case get_closest_peer(Group) of
        {error, {no_process, _}} -> ok;
        {error, Reason}          -> {error, Reason};
        Peer                     -> ppg_peer:broadcast(Peer, Message)
    end.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec get_closest_peer(name()) -> pid() | {error, Reason} when
      Reason :: {no_process, name()} | {no_such_group, name()}.
get_closest_peer(Group) ->
    case pg2:get_closest_pid(?PG2_NAME(Group)) of
        {error, {no_process, _}}    -> {error, {no_process, Group}};
        {error, {no_such_group, _}} -> {error, {no_such_group, Group}};
        ContactPeer                 ->
            case ppg_peer_sup:get_random_child(Group) of
                undefined -> ContactPeer;
                Peer      -> Peer
            end
    end.
