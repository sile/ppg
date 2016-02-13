%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
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
-export([which_children/0]).

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

-spec start_child(ppg:name()) -> {ok, pid()} | {error, Reason} when
      Reason :: {already_started, pid()} | term().
start_child(Group) ->
    Child = #{id => Group, start => {ppg_peer_sup, start_link, [Group]}, type => supervisor},
    supervisor:start_child(?MODULE, Child).

-spec stop_child(ppg:name()) -> ok.
stop_child(Group) ->
    _ = supervisor:terminate_child(?MODULE, Group),
    _ = supervisor:delete_child(?MODULE, Group),
    ok.

-spec which_children() -> [{ppg:name(), pid()|restarting}].
which_children() ->
    [{Group, Pid} || {Group, Pid, _, _} <- supervisor:which_children(?MODULE)].

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([]) ->
    {ok, {#{strategy => one_for_one}, []}}.
