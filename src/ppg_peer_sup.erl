%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Supervisor for ppg_peer processes
%% @private
-module(ppg_peer_sup).

-behaviour(supervisor).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/1]).
-export([push_member/3]).
-export([pop_member/2]).
-export([get_peer/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(ppg:name()) -> {ok, pid()} | {error, Reason} when
      Reason :: {already_started, pid()} | term().
start_link(Group) ->
    supervisor:start_link(sup_name(Group), ?MODULE, []).

%% NOTE: This function is executed by `Member' (i.e., serialized)
-spec push_member(ppg:name(), ppg:member(), ppg:join_options()) -> {ok, pid()} | {error, Reason::term()}.
push_member(Group, Member, Options) ->
    Count = length(ppg_local_ns:which_processes({peer, Group, Member, '_'})),
    Name = ppg_local_ns:otp_name({peer, Group, Member, Count}),
    supervisor:start_child(sup_name(Group), [Name, Group, Member, Options]).

%% NOTE: This function is executed by `Member' (i.e., serialized)
-spec pop_member(ppg:name(), ppg:member()) -> ok.
pop_member(Group, Member) ->
    %% TODO: assert(Count > 0)
    Count = length(ppg_local_ns:which_processes({peer, Group, Member, '_'})),
    Name = ppg_local_ns:otp_name({peer, Group, Member, Count - 1}),
    ok = ppg_peer:stop(Name),
    ok.

-spec get_peer(ppg:name(), ppg:member()) -> pid().
get_peer(Group, Member) ->
    case ppg_local_ns:whereis_name({peer, Group, Member, 0}) of
        undefined -> error(badarg, [Group, Member]);
        Peer      -> Peer
    end.

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([]) ->
    Child = #{id => peer, start => {ppg_peer, start_link, []}, restart => temporary},
    {ok, {#{strategy => simple_one_for_one}, [Child]}}.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec sup_name(ppg:name()) -> local:otp_name().
sup_name(Group) ->
    ppg_local_ns:otp_name({group, Group}).
