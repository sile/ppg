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
-export([which_children/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, Reason::term()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_child(ppg:name(), ppg:member(), ppg:join_options()) -> {ok, ppg_peer:peer()} | {error, Reason} when
      Reason :: {no_such_group, ppg:name()} | term().
start_child(Group, Member, Options) ->
    case ppg_group_sup:find_child(Group) of
        error     -> {error, {no_such_group, Group}};
        {ok, Sup} -> supervisor:start_child(Sup, [Group, Member, Options])
    end.

-spec which_children(pid()) -> [ppg_peer:peer()].
which_children(Sup) ->
    [P || {_, P, _, _} <- supervisor:which_children(Sup)].

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([]) ->
    Child = #{id => peer, start => {ppg_peer, start_link, []}, restart => temporary},
    {ok, {#{strategy => simple_one_for_one}, [Child]}}.
