%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
-module(ppg_tests).

-include_lib("eunit/include/eunit.hrl").

%%----------------------------------------------------------------------------------------------------------------------
%% Unit Tests
%%----------------------------------------------------------------------------------------------------------------------
group_test_() ->
    foreach(
      [],
      [
       {"Creates and deletes an empty group",
        fun () ->
                ?assertEqual(ok, ppg:create(foo)),
                ?assertEqual([foo], ppg:which_groups()),

                ?assertEqual(ok, ppg:delete(foo)),
                ?assertEqual([], ppg:which_groups())
        end},
       {"Duplicative creations/deletions of the same group are ignored",
        fun () ->
                ok = ppg:create(foo),
                ?assertEqual(ok, ppg:create(foo)),
                ?assertEqual([foo], ppg:which_groups()),

                ok = ppg:delete(foo),
                ?assertEqual(ok, ppg:delete(foo)),
                ?assertEqual([], ppg:which_groups())
        end},
       {"Multiple groups",
        fun () ->
                Groups = [foo, bar, baz, qux],
                ok = lists:foreach(fun (Group) -> ?assertEqual(ok, ppg:create(Group)) end, Groups),
                ?assertEqual(lists:sort(Groups), lists:sort(ppg:which_groups())),

                ok = lists:foreach(fun (Group) -> ?assertEqual(ok, ppg:delete(Group)) end, Groups),
                ?assertEqual([], ppg:which_groups())
        end}
      ]).

join_test_() ->
    Group = foo,
    foreach(
      [Group],
      [
       {"Joins in a group",
        fun () ->
                ?assertEqual(ok, ppg:join(Group, self())),
                ?assertEqual([self()], ppg:get_members(Group))
        end}
      ]).

member_test_() ->
    Group = foo,
    foreach(
      [Group],
      [
       {"Initially, a group has no members",
        fun () ->
                ?assertEqual([], ppg:get_members(Group)),
                ?assertEqual([], ppg:get_local_members(Group)),
                ?assertEqual([], ppg:get_graph(Group)),
                ?assertEqual({error, {no_process, Group}}, ppg:get_closest_pid(Group))
        end}
      ]).

broadcast_test_() ->
    Group = foo,
    foreach(
      [Group],
      [
       {"Broadcasts to an empty group",
        fun () ->
                ?assertEqual(ok, ppg:broadcast(Group, hello)),
                receive hello -> ?assert(false) after 20 -> ?assert(true) end
        end},
       {"Broadcasts to a single member group",
        fun () ->
                ok = ppg:join(Group, self()),
                ?assertEqual(ok, ppg:broadcast(Group, hello)),
                receive hello -> ?assert(true) after 20 -> ?assert(false) end,

                ok = ppg:leave(Group, self()),
                ?assertEqual(ok, ppg:broadcast(Group, hello)),
                receive hello -> ?assert(false) after 20 -> ?assert(true) end
        end},
       {"Broadcasts to a two member group",
        fun () ->
                ok = ppg:join(Group, self()),
                ok = ppg:join(Group, self()),
                timer:sleep(3000), % TODO: delete

                ?assertEqual(ok, ppg:broadcast(Group, hello)),
                receive hello -> ?assert(true) after 20 -> ?assert(false) end,
                receive hello -> ?assert(true) after 20 -> ?assert(false) end
        end}
      ]).

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
foreach(PreDefinedGroups, TestCases) ->
    {foreach,
     fun () ->
             {ok, Apps} = application:ensure_all_started(ppg),
             ok = lists:foreach(fun (Group) -> ok = ppg:create(Group) end, PreDefinedGroups),
             Apps
     end,
     fun (Apps) ->
             ok = lists:foreach(fun (App) -> ok = application:stop(App) end, Apps)
     end,
     TestCases}.
