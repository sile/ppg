%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Peer Sampling Service Interface
%% @private
-module(ppg_peer_sampling_service).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/2]).
-export([is_instance/1]).
-export([is_callback_module/1]).
-export([join/2]).
-export([leave/1]).
-export([get_peers/1]).
-export([handle_info/2]).
-export([notify_neighbor_up/1]).
-export([notify_neighbor_down/1]).

-export_type([instance/0]).
-export_type([callback_module/0]).
-export_type([state/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-opaque instance() :: {callback_module(), state()}.

-type callback_module() :: module().
-type state() :: term().

%%----------------------------------------------------------------------------------------------------------------------
%% Behaviour Callbacks
%%----------------------------------------------------------------------------------------------------------------------
-callback join(pid(), state()) -> state().
-callback leave(state()) -> state().
-callback get_peers(state()) -> {[pid()], state()}.
-callback handle_info(term(), state()) -> {ok, state()} | {error, Reason::term()} | ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-spec new(callback_module(), state()) -> instance().
new(Module, State) ->
    _ = is_callback_module(Module) orelse error(badarg, [Module, State]),
    {Module, State}.

-spec is_instance(instance() | term()) -> boolean().
is_instance({Module, _}) -> is_callback_module(Module);
is_instance(_)           -> false.

-spec is_callback_module(callback_module() | term()) -> boolean().
is_callback_module(Module) ->
    (is_atom(Module) andalso
     ppg_util:function_exported(Module, join, 2) andalso
     ppg_util:function_exported(Module, leave, 1) andalso
     ppg_util:function_exported(Module, get_peers, 1) andalso
     ppg_util:function_exported(Module, handle_info, 2)).

-spec join(pid(), instance()) -> instance().
join(ContactPeer, {Module, State0}) ->
    State1 = Module:join(ContactPeer, State0),
    {Module, State1}.

-spec leave(instance()) -> instance().
leave({Module, State0}) ->
    State1 = Module:leave(State0),
    {Module, State1}.

-spec get_peers(instance()) -> {[pid()], instance()}.
get_peers({Module, State0}) ->
    {Peers, State1} = Module:get_peers(State0),
    {Peers, {Module, State1}}.

-spec handle_info(term(), instance()) -> {ok, instance()} | {error, Reason::term()} | ignore.
handle_info(Info, {Module, State0}) ->
    case Module:handle_info(Info, State0) of
        {ok, State1} -> {ok, {Module, State1}};
        Other        -> Other
    end.

-spec notify_neighbor_up(pid()) -> ok.
notify_neighbor_up(Peer) ->
    _ = self() ! {'NEIGHBOR_UP', Peer},
    ok.

-spec notify_neighbor_down(pid()) -> ok.
notify_neighbor_down(Peer) ->
    _ = self() ! {'NEIGHBOR_DOWN', Peer},
    ok.
