%% Copyright (c) 2016, Takeru Ohta <phjgt308@gmail.com>
%%
%% This software is released under the MIT License.
%% See the LICENSE file in the project root for full license information.
%%
%% @doc Utility Functions
%% @private
-module(ppg_util).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([is_pos_integer/1]).
-export([is_timeout/1]).
-export([cancel_and_flush_timer/2]).
-export([cancel_and_send_after/4]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @doc Returns `true' if `X' is a positive integer, otherwise `false'
-spec is_pos_integer(X :: (pos_integer() | term())) -> boolean().
is_pos_integer(X) -> is_integer(X) andalso X > 0.

%% @doc Returns `true' if `X' is a `timeout()' instance, otherwise `false'
-spec is_timeout(X :: (timeout() | term())) -> boolean().
is_timeout(infinity) -> true;
is_timeout(X)        -> is_integer(X) andalso X >= 0.

%% @doc Cancels `Timer' and removes `FlushMessage' from caller's mailbox if exists
-spec cancel_and_flush_timer(reference(), term()) -> ok.
cancel_and_flush_timer(Timer, FlushMessage) ->
    _ = erlang:cancel_timer(Timer),
    receive
        FlushMessage -> ok
    after 0 -> ok
    end.

%% @doc Combination of {@link cancel_and_flush_timer/2} and {@link erlang:send_after/3}
-spec cancel_and_send_after(reference(), timeout(), pid(), term()) -> reference().
cancel_and_send_after(OldTimer, Time, Pid, Message) ->
    _ = case Pid =:= self() of
            true  -> cancel_and_flush_timer(OldTimer, Message);
            false -> erlang:cancel_timer(OldTimer)
        end,
    erlang:send_after(Time, Pid, Message).
