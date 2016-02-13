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
              View = ppg_hyparview:new(Group),
              ?assert(ppg_hyparview:is_view(View))
      end},
     {"The initial view is empty",
      fun () ->
              View = ppg_hyparview:new(Group),
              ?assertEqual([], ppg_hyparview:get_peers(View))
      end}
    ].
