%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc This module provides debugging functionalities
-module(ppg_debug).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([get_graph/1, get_graph/2]).
-export([broadcast/2]).

%% TODO: generate dot formatted graph
%% TODO: random test function

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @equiv get_graph(Group, 5000)
-spec get_graph(ppg:name()) -> ppg_peer:graph().
get_graph(Group) ->
    get_graph(Group, 5000).

-spec get_graph(ppg:name(), timeout()) -> ppg_peer:graph().
get_graph(Group, Timeout) ->
    {ok, {_, Peer}} = ppg:get_closest_member(Group),
    ppg_peer:get_graph(Peer, Timeout).

-spec broadcast(ppg:name(), ppg:message()) -> ok.
broadcast(Group, Message) ->
    {ok, {_, Peer}} = ppg:get_closest_member(Group),
    ppg:broadcast(Peer, Message).
