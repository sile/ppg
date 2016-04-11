%% Copyright (c) 2016, Takeru Ohta <phjgt308@gmail.com>
%%
%% This software is released under the MIT License.
%% See the LICENSE file in the project root for full license information.
%%
%% @doc Application Module
%% @private
-module(ppg_app).

-behaviour(application).

%%----------------------------------------------------------------------------------------------------------------------
%% 'application' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([start/2, stop/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'application' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
start(_StartType, _StartArgs) ->
    ppg_sup:start_link().

%% @private
stop(_State) ->
    ok.
