%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Auxiliary functions for maps
%% @private
-module(ppg_maps).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([foreach/2]).
-export([filtermap/2]).
-export([count/2]).
-export([any/2]).
-export([all/2]).
-export([partition/2]).
-export([random_key/1, random_key/2]).
-export([random_key_with_favoritism/2]).
-export([random_keys/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec foreach(fun ((term(), term()) -> any()), #{}) -> ok.
foreach(Fun, Map) ->
    maps:fold(fun (K, V, _) -> _ = Fun(K, V), ok end, ok, Map).

-spec filtermap(fun ((term(), term()) -> boolean() | {true, term()}), #{}) -> #{}.
filtermap(Fun, Map) ->
    maps:fold(
      fun (K, V, Acc) ->
              case Fun(K, V) of
                  false     -> Acc;
                  true      -> maps:put(K, V, Acc);
                  {true, W} -> maps:put(K, W, Acc)
              end
      end,
      #{},
      Map).

-spec count(fun ((term(), term()) -> boolean()), #{}) -> non_neg_integer().
count(Fun, Map) ->
    maps:fold(
      fun (K, V, Count) ->
              case Fun(K, V) of
                  true  -> Count + 1;
                  false -> Count
              end
      end,
      0,
      Map).

-spec any(fun ((term(), term()) -> boolean()), #{}) -> boolean().
any(Fun, Map) ->
    maps:fold(fun (_, _, true) -> true;
                  (K, V, _)    -> Fun(K, V)
              end,
              false,
              Map).

-spec all(fun ((term(), term()) -> boolean()), #{}) -> boolean().
all(Fun, Map) ->
    maps:fold(fun (_, _, false) -> false;
                  (K, V, _)     -> Fun(K, V)
              end,
              true,
              Map).

-spec partition(fun ((term(), term()) -> boolean()), #{}) -> {Satisfying::#{}, NotSatisfying::#{}}.
partition(Fun, Map) ->
    maps:fold(fun (K, V, {Satisfying, NotSatisfying}) ->
                      case Fun(K, V) of
                          true  -> {maps:put(K, V, Satisfying), NotSatisfying};
                          false -> {Satisfying, maps:put(K, V, NotSatisfying)}
                      end
              end,
              {#{}, #{}},
              Map).

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

-spec random_key_with_favoritism(fun ((term(), term()) -> boolean()), #{}) -> {ok, term()} | error.
random_key_with_favoritism(Fun, Map) ->
    {Priors, Posteriors} = partition(Fun, Map),
    case random_key(Priors) of
        {ok, Key} -> {ok, Key};
        error     -> random_key(Posteriors)
    end.

-spec random_keys(non_neg_integer(), #{}) -> [term()].
random_keys(N, Map) ->
    List = maps:fold(fun (K, _, Acc) -> [{rand:uniform(), K} | Acc] end, [], Map),
    [K || {_, K} <- lists:sublist(lists:sort(List), N)].
