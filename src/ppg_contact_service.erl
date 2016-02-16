%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(ppg_contact_service).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/1]).
-export([get_peer/1]).

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

-spec get_peer(service()) -> ppg_peer:peer().
get_peer(Service = #?STATE{group = Group}) ->
    GlobalName = {?MODULE, Group},
    case global:whereis_name(GlobalName) of
        undefined ->
            _ = global:register_name(GlobalName, self()),
            get_peer(Service);
        Peer ->
            Peer
    end.
