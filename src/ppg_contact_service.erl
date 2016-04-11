%% Copyright (c) 2016, Takeru Ohta <phjgt308@gmail.com>
%%
%% This software is released under the MIT License.
%% See the LICENSE file in the project root for full license information.
%%
%% @doc Peer Contact Service
%% @private
-module(ppg_contact_service).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/1]).
-export([get_peer/1]).
-export([find_peer/1]).

-export_type([service/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(STATE, ?MODULE).

-record(?STATE,
        {
          group :: ppg:name()
        }).

-opaque service() :: #?STATE{}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @doc Creates a new contact service instance(client) which associated with `Group'
-spec new(ppg:name()) -> service().
new(Group) ->
    #?STATE{group = Group}.

%% @doc Finds the peer which behaves as the contact service
-spec find_peer(service()) -> {ok, ppg_peer:peer()} | error.
find_peer(#?STATE{group = Group}) ->
    case evel:find_leader({?MODULE, Group}) of
        error           -> error;
        {ok, {Peer, _}} -> {ok, Peer}
    end.

%% @doc Gets the peer which behaves as the contact service
%%
%% If there is no such peer, the caller process will try to become the contact service
-spec get_peer(service()) -> ppg_peer:peer().
get_peer(Service = #?STATE{group = Group}) ->
    case find_peer(Service) of
        {ok, Peer} -> Peer;
        error      ->
            {Peer, _} = evel:elect({?MODULE, Group}, self(), [{link, false}]),
            Peer
    end.
