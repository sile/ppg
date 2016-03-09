%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Message Transporter
%% @private
-module(ppg_transporter).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/2, new/3]).
-export([connect/2]).
-export([close/2]).
-export([async_send/3]).
-export([handle_info/2]).
-export([get_self_address/1]).

-export_type([transporter/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-define(TRANSPORTER, ?MODULE).
-record(?TRANSPORTER,
        {
          module            :: ppg_transport:callback_module(),
          self_address      :: ppg_transport:address(),
          options = []      :: [ppg_transport:option()],
          connections = #{} :: #{ppg_transport:connection_id() => ppg_transport:connection()} % TODO: アドレスも持つ(?)
        }).

-opaque transporter() :: #?TRANSPORTER{}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new(ppg_transport:callback_module(), ppg_transport:address()) -> transporter().
new(Module, Address) ->
    new(Module, Address, []).

-spec new(ppg_transport:callback_module(), ppg_transport:address(), [ppg_transport:option()]) -> transporter().
new(Module, Address, Options) ->
    #?TRANSPORTER{module = Module, self_address = Address, options = Options}.

-spec get_self_address(transporter()) -> ppg_transport:address().
get_self_address(#?TRANSPORTER{self_address = Address}) ->
    Address.

-spec connect(ppg_transport:address(), transporter()) ->
                     {ok, ppg_transport:connection_id(), transporter()} | {error, Reason::term()}.
connect(Address, Transporter) ->
    case (Transporter#?TRANSPORTER.module):connect(Address, Transporter#?TRANSPORTER.options) of
        {error, Reason}            -> {error, Reason};
        {ok, {Id, _} = Connection} -> {ok, Id, add_connection(Connection, Transporter)}
    end.

-spec close(ppg_transport:connection_id(), transporter()) -> transporter().
close(Id, Transporter0) ->
    case remove_connection(Id, Transporter0) of
        error                          -> Transporter0;
        {ok, Connection, Transporter1} ->
            ok = (Transporter1#?TRANSPORTER.module):close(Connection),
            Transporter1
    end.

-spec async_send(ppg_transport:connection_id(), ppg_transport:message(), transporter()) -> ok.
async_send(Id, Message, Transporter) ->
    case maps:find(Id, Transporter#?TRANSPORTER.connections) of
        error            -> Transporter;
        {ok, Connection} -> (Transporter#?TRANSPORTER.module):async_send(Connection, Message)
    end.

-spec handle_info(term(), transporter()) ->
                         ignore |
                         {connected, ppg_transport:connection_id(), transporter()} |
                         {closed, ppg_transport:connection_id(), transporter()} |
                         {recv, ppg_transport:connection_id(), ppg_transport:message()} |
                         {recv_multi, ppg_transport:connection_id(), [ppg_transport:message()]}.
handle_info(Info, Transporter0) ->
    case (Transporter0#?TRANSPORTER.module):handle_info(Info) of
        ignore ->
            ignore;
        {connected, {Id, _} = Connection} ->
            Transporter1 = add_connection(Connection, Transporter0),
            {connected, Id, Transporter1};
        {closed, {id, Id}} ->
            case remove_connection(Id, Transporter0) of
                error                 -> ignore;
                {ok, _, Transporter1} -> {closed, Id, Transporter1}
            end;
        {closed, {data, Data}} ->
            case lists:keyfind(Data, 2, maps:values(Transporter0#?TRANSPORTER.connections)) of
                false   -> ignore;
                {Id, _} -> handle_info({closed, {id, Id}}, Transporter0)
            end;
        {recv, Id, Message} ->
            case maps:is_key(Id, Transporter0#?TRANSPORTER.connections) of
                false -> ignore;
                true  -> {recv, Id, Message}
            end;
        {recv_multi, Id, Messages} ->
            case maps:is_key(Id, Transporter0#?TRANSPORTER.connections) of
                false -> ignore;
                true  -> {recv_multi, Id, Messages}
            end
    end.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec add_connection(ppg_transport:connection(), transporter()) -> transporter().
add_connection({Id, _} = Connection, Transporter) ->
    false = maps:is_key(Id, Transporter#?TRANSPORTER.connections), % assertion
    Connections = maps:put(Id, Connection, Transporter#?TRANSPORTER.connections),
    Transporter#?TRANSPORTER{connections = Connections}.

-spec remove_connection(ppg_transport:connection_id(), transporter()) ->
                               {ok, ppg_transport:connection(), transporter()} | error.
remove_connection(Id, Transporter) ->
    case maps:find(Id, Transporter#?TRANSPORTER.connections) of
        error            -> error;
        {ok, Connection} ->
            Connections = maps:remove(Connection, Transporter#?TRANSPORTER.connections),
            {ok, Connection, Transporter#?TRANSPORTER{connections = Connections}}
    end.
