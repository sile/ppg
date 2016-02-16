%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Utility Functions
%% @private
-module(ppg_util).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([is_pos_integer/1]).
-export([is_local_pid/1]).
-export([is_timeout/1]).
-export([proplist_to_record/3]).
-export([function_exported/3]).
-export([cancel_and_flush_timer/2]).
-export([cancel_and_send_after/4]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec is_pos_integer(pos_integer() | term()) -> boolean().
is_pos_integer(X) -> is_integer(X) andalso X > 0.

-spec is_local_pid(pid() | term()) -> boolean().
is_local_pid(X) -> is_pid(X) andalso node(X) =:= node().

-spec is_timeout(timeout() | term()) -> boolean().
is_timeout(infinity) -> true;
is_timeout(X)        -> is_integer(X) andalso X >= 0.

-spec proplist_to_record(atom(), [atom()], [{atom(), term()}]) -> tuple().
proplist_to_record(RecordName, Fields, List) ->
    list_to_tuple(
      [RecordName |
       [case lists:keyfind(Field, 1, List) of
            false      -> error(badarg, [RecordName, Fields, List]);
            {_, Value} -> Value
        end || Field <- Fields]]).

%% @doc Equivalent to {@link erlang:function_exported/3} except `Module' will be loaded if it has not been loaded
-spec function_exported(module(), atom(), arity()) -> boolean().
function_exported(Module, Function, Arity) ->
    _ = is_atom(Module) orelse error(badarg, [Module, Function, Arity]),
    _ = is_atom(Function) orelse error(badarg, [Module, Function, Arity]),
    _ = (is_integer(Arity) andalso Arity >= 0) orelse error(badarg, [Module, Function, Arity]),
    _ = code:is_loaded(Module) =/= false orelse code:load_file(Module),
    erlang:function_exported(Module, Function, Arity).

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
