%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
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
-export([now_ms/0]).

-export_type([milliseconds/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-type milliseconds() :: non_neg_integer().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec is_pos_integer(pos_integer() | term()) -> boolean().
is_pos_integer(X) -> is_integer(X) andalso X > 0.

-spec is_timeout(timeout() | term()) -> boolean().
is_timeout(infinity) -> true;
is_timeout(X)        -> is_integer(X) andalso X >= 0.

-spec cancel_and_flush_timer(reference(), term()) -> ok.
cancel_and_flush_timer(Timer, FlushMessage) ->
    _ = erlang:cancel_timer(Timer),
    receive
        FlushMessage -> ok
    after 0 -> ok
    end.

-spec cancel_and_send_after(reference(), timeout(), pid(), term()) -> reference().
cancel_and_send_after(OldTimer, Time, Pid, Message) ->
    _ = case Pid =:= self() of
            true  -> cancel_and_flush_timer(OldTimer, Message);
            false -> erlang:cancel_timer(OldTimer)
        end,
    erlang:send_after(Time, Pid, Message).

-spec now_ms() -> milliseconds().
now_ms() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000 * 1000 * 1000) + (Sec * 1000) + (Micro div 1000).
