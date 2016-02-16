%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
%% @private
-module(ppg_maps).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([foreach/2]).
-export([random_key/1, random_key/2]).
-export([random_keys/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec foreach(fun ((term(), term()) -> any()), #{}) -> ok.
foreach(Fun, Map) ->
    maps:fold(fun (K, V, _) -> _ = Fun(K, V), ok end, ok, Map).

-spec random_key(#{}) -> {ok, term()} | error.
random_key(Map) ->
    case maps:size(Map) of
        0 -> error;
        N ->
            maps:fold(
              fun (K, _, {rest, 0}) -> {ok, K};
                  (_, _, {rest, I}) -> {rest, I - 1};
                  (_, _, {ok, K})   -> {ok, K}
              end,
              {rest, rand:uniform(N) - 1},
              Map)
    end.

-spec random_key(#{}, term()) -> term().
random_key(Map, Default) ->
    case random_key(Map) of
        error     -> Default;
        {ok, Key} -> Key
    end.

-spec random_keys(non_neg_integer(), #{}) -> [term()].
random_keys(N, Map) ->
    List = maps:fold(fun (K, _, Acc) -> [{rand:uniform(), K} | Acc] end, [], Map),
    [K || {_, K} <- lists:sublist(lists:sort(List), N)].
