%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
-module(ppg_hyparview_tests).

-include_lib("eunit/include/eunit.hrl").

%%----------------------------------------------------------------------------------------------------------------------
%% Unit Tests
%%----------------------------------------------------------------------------------------------------------------------
new_test_() ->
    Group = foo,
    [
     {"Creates an instance",
      fun () ->
              ppg_hyparview:new(Group, []),
              ?assert(true)
      end}
    ].
