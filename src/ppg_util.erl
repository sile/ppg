%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Utility Functions
%% @private
-module(ppg_util).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([is_pos_integer/1]).
-export([proplist_to_record/3]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec is_pos_integer(pos_integer() | term()) -> boolean().
is_pos_integer(X) -> is_integer(X) andalso X > 0.

-spec proplist_to_record(atom(), [atom()], [{atom(), term()}]) -> tuple().
proplist_to_record(RecordName, Fields, List) ->
    list_to_tuple(
      [RecordName |
       [case lists:keyfind(Field, 1, List) of
            false      -> error(badarg, [RecordName, Fields, List]);
            {_, Value} -> {Field, Value}
        end || Field <- Fields]]).
