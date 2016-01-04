%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Application Local Name Server
%% @private
-module(ppg_local_ns).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([child_spec/0]).
-export([otp_name/1]).
-export([which_processes/1]).

-export_type([raw_name/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(NS_NAME, ?MODULE).

-type raw_name() :: term().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    local:name_server_child_spec(?NS_NAME).

-spec otp_name(raw_name()) -> local:otp_name().
otp_name(Name) ->
    local:otp_name({?NS_NAME, Name}).

-spec which_processes(ets:match_pattern()) -> [{raw_name(), pid()}].
which_processes(Pattern) ->
    local:which_processes(?NS_NAME, Pattern).
