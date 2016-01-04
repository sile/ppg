%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Supervisor for ppg_peer processes
%% @private
-module(ppg_peer_sup).

-behaviour(supervisor).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/0]).
-export([start_child/3]).
-export([get_random_child/1]).
-export([which_children/1]).
-export([select_children_by_destination/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, Reason::term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(ppg:name(), pid(), ppg:join_options()) -> {ok, pid()} | {error, {no_such_group, ppg:name()}}.
start_child(Group, Destination, Options) ->
    Name = ppg_local_ns:otp_name({peer, Group, Destination, erlang:unique_integer()}),
    supervisor:start_child(?MODULE, [Name, Group, Destination, Options]).

-spec get_random_child(ppg:name()) -> pid() | undefined.
get_random_child(Group) ->
    case [Pid || {_, Pid} <- ppg_local_ns:which_processes({peer, Group, '_', '_'})] of
        []    -> undefined;
        Peers -> lists:nth(rand:uniform(length(Peers)), Peers)
    end.

-spec which_children(ppg:name()) -> [{Peer::pid(), Destination::pid()}].
which_children(Group) ->
    [{Peer, Destination} || {{_, _, Destination, _}, Peer} <- ppg_local_ns:which_processes({peer, Group, '_', '_'})].

-spec select_children_by_destination(ppg:name(), pid()) -> [pid()].
select_children_by_destination(Group, Destination) ->
    [Peer || {_, Peer} <- ppg_local_ns:which_processes({peer, Group, Destination, '_'})].

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([]) ->
    Child = #{id => peer, start => {ppg_peer, start_link, []}, restart => temporary},
    {ok, {#{strategy => simple_one_for_one}, [Child]}}.
