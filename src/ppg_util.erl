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
-export([delete_random/1]). % TODO:
-export([select_random/1]).
-export([random_pop/1]).
-export([random_select/1]).
-export([random_select_key/1]).
-export([random_select_keys/2]).

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

-spec delete_random(list()) -> {term(), list()}.
delete_random(List) ->
    I = rand:uniform(length(List)) - 1,
    {Front, [Deleted | Rear]} = lists:split(I, List),
    {Deleted, Front ++ Rear}.

-spec select_random(list()) -> term().
select_random(List) ->
    lists:nth(rand:uniform(length(List)), List).

-spec random_pop(#{}) -> {{Key::term(), Value::term()}, #{}}.
random_pop(Map) ->
    _ = maps:size(Map) > 0 orelse error(badarg, [Map]),
    maps:fold(
      fun (K, V, {0, Acc})                    -> {{K, V}, Acc};
          (K, V, {I, Acc}) when is_integer(I) -> {I - 1, maps:put(K, V, Acc)};
          (K, V, {Popped, Acc})               -> {Popped, maps:put(K, V, Acc)}
      end,
      {rand:uniform(maps:size(Map)) - 1, maps:new()},
      Map).

-spec random_select(#{}) -> {Key::term(), Value::term()}.
random_select(Map) ->
    _ = maps:size(Map) > 0 orelse error(badarg, [Map]),
    maps:fold(
      fun (K, V, 0)                    -> {K, V};
          (_, _, I) when is_integer(I) -> I - 1;
          (_, _, Selected)             -> Selected
      end,
      rand:uniform(maps:size(Map)) - 1,
      Map).

-spec random_select_key(#{}) -> term().
random_select_key(Map) ->
    element(1, random_select(Map)).

-spec random_select_keys(pos_integer(), #{}) -> [term()].
random_select_keys(N, Map) ->
    lists:sublist(shuffle(maps:keys(Map)), N).

-spec shuffle(list()) -> list().
shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- List])].
