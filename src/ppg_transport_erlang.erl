%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(ppg_transport_erlang).

-behaviour(ppg_transport).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new_transporter/0]).

-export_type([address/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'ppg_transport' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([connect/2, close/1, async_send/2, handle_info/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types & Records
%%----------------------------------------------------------------------------------------------------------------------
-type address() :: ppg_peer:peer().

-record(data,
        {
          address :: address(),
          monitor :: reference()
        }).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new_transporter() -> ppg_transporter:transporter().
new_transporter() ->
    ppg_transporter:new(?MODULE, self(), []).

%%----------------------------------------------------------------------------------------------------------------------
%% 'ppg_transport' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
-spec connect(address(), [ppg_transport:option()]) -> {ok, ppg_transport:connection()}.
connect(Address, _Options) ->
    Id = make_ref(),
    Data = new_data(Address),
    {ok, {Id, Data}}.

%% @private
-spec close(ppg_transport:connection()) -> ok.
close({_, Data}) ->
    _ = demonitor(Data#data.monitor, [flush]),
    ok.

%% @private
-spec async_send(ppg_transport:connection(), ppg_transport:message()) -> ok.
async_send({Id, Data}, Message) ->
    _ = Data#data.address ! {Id, Message},
    ok.

%% @private
-spec handle_info(term()) -> ppg_transport:handle_info_result().
handle_info({'DOWN', Monitor, _, Address, _})    -> {closed, {data, #data{address = Address, monitor = Monitor}}};
handle_info({'CONNECT', Address, Id})            -> {connected, {Id, new_data(Address)}};
handle_info({Id, Message}) when is_reference(Id) -> {recv, Id, Message};
handle_info(_)                                   -> ignore.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new_data(address()) -> #data{}.
new_data(Address) ->
    #data{address = Address, monitor = monitor(process, Address)}.
