%% Copyright (c) 2016, Takeru Ohta <phjgt308@gmail.com>
%%
%% This software is released under the MIT License.
%% See the LICENSE file in the project root for full license information.
%%
%% @doc Supervisor for local groups
%% @private
-module(ppg_group_sup).

-behaviour(supervisor).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/0]).
-export([start_child/1]).
-export([stop_child/1]).
-export([find_child/1]).
-export([which_children/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @doc Starts the supervisor
-spec start_link() -> {ok, pid()} | {error, Reason::term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Starts a new child
-spec start_child(ppg:name()) -> {ok, pid()} | {error, Reason} when
      Reason :: {already_started, pid()} | term().
start_child(Group) ->
    Child = #{id => Group, start => {ppg_peer_sup, start_link, []}, type => supervisor},
    supervisor:start_child(?MODULE, Child).

%% @doc Stops the child
-spec stop_child(ppg:name()) -> ok.
stop_child(Group) ->
    _ = supervisor:terminate_child(?MODULE, Group),
    _ = supervisor:delete_child(?MODULE, Group),
    ok.

%% @doc Finds the child which has the id `Group'
-spec find_child(ppg:name()) -> {ok, pid()} | error.
find_child(Group) ->
    case lists:keyfind(Group, 1, which_children()) of
        false           -> error;
        {_, restarting} -> _ = timer:sleep(1), find_child(Group);
        {_, Pid}        -> {ok, Pid}
    end.

%% @doc Returns the list of existing children
-spec which_children() -> [{ppg:name(), pid()|restarting}].
which_children() ->
    [{Group, Pid} || {Group, Pid, _, _} <- supervisor:which_children(?MODULE)].

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([]) ->
    {ok, {#{strategy => one_for_one}, []}}.
