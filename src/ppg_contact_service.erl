%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Peer Contact Service
%%
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
-spec new(ppg:name()) -> service().
new(Group) ->
    #?STATE{group = Group}.

-spec find_peer(service()) -> {ok, ppg:peer()} | error.
find_peer(#?STATE{group = Group}) ->
    case global:whereis_name({?MODULE, Group}) of
        undefined -> error;
        Peer      -> {ok, Peer}
    end.

-spec get_peer(service()) -> ppg:peer().
get_peer(Service = #?STATE{group = Group}) ->
    case find_peer(Service) of
        {ok, Peer} -> Peer;
        error      ->
            _ = global:register_name({?MODULE, Group}, self()),
            get_peer(Service)
    end.
