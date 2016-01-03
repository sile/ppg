%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
-module(ppg_app_tests).

-include_lib("eunit/include/eunit.hrl").

%%----------------------------------------------------------------------------------------------------------------------
%% Unit Tests
%%----------------------------------------------------------------------------------------------------------------------
start_test_() ->
    [
     {"Starts and stops ppg application",
      fun () ->
              Result = application:ensure_all_started(ppg),
              ?assertMatch({ok, _}, Result),
              lists:foreach(
                fun (App) ->
                        ?assertEqual(ok, application:stop(App))
                end,
                element(2, Result))
      end}
    ].
