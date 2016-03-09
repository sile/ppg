%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Transport Layer Interface
-module(ppg_transport).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export_type([callback_module/0]).
-export_type([option/0]).
-export_type([address/0]).
-export_type([connection/0]).
-export_type([connection_id/0]).
-export_type([connection_data/0]).
-export_type([message/0]).
-export_type([handle_info_result/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Behaviour Callbacks
%%----------------------------------------------------------------------------------------------------------------------
-callback connect(address(), [option()]) -> {ok, connection()} | {error, Reason::term()}.
-callback close(connection()) -> ok.
-callback async_send(connection(), message())-> ok.
-callback handle_info(term()) -> handle_info_result().

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-type callback_module() :: module().
-type option() :: term().
-type address() :: term().
-type connection() :: {connection_id(), connection_data()}.
-type connection_id() :: term().
-type connection_data() :: term().
-type message() :: term().

-type handle_info_result() ::
        ignore
      | {connected, connection()}
      | {closed, {id, connection_id()} | {data, connection_data()}}
      | {recv, connection_id(), message()}
      | {recv_multi, connection_id(), [message()]}.
