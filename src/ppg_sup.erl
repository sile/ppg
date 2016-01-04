%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Root Supervisor
%% @private
-module('ppg_sup').

-behaviour(supervisor).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/0]).

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

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([]) ->
    Children =
        [
         ppg_local_ns:child_spec(),
         #{id => peer_sup, start => {ppg_peer_sup, start_link, []}, type => supervisor}
        ],
    {ok, {#{strategy => rest_for_one}, Children}}.
