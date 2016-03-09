%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc Internal Event Queue
%% @private
-module(ppg_event_queue).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([new/0]).
-export([enqueue/2, enqueue/3]).
-export([dequeue/1]).

-export_type([queue/0]).
-export_type([event/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-opaque queue() :: queue:queue(event()).

-type event() :: {close, ppg_transport:connection_id()}
               | {disconnect, ppg_transport:address()} % TODO: closeと統合する
               | {disconnected, ppg_transport:from(), IsDown::boolean()}
               | {neighbor_up, ppg_transport:from()}
               | {neighbor_down, ppg_transport:from()}
               | {send, ppg_transport:from(), ppg_transport:message()}
               | {recv, ppg_transport:from(), ppg_transport:message()}
               | {hyparview, term()} % TOOD
               | {plumtree, term()}. % TODO

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec new() -> queue().
new() ->
    queue:new().

-spec enqueue(event(), queue()) -> queue().
enqueue(Event, Queue) ->
    queue:in(Event, Queue).

-spec enqueue(event(), timeout(), queue()) -> queue().
enqueue(Event, After, Queue) ->
    error(unimplemented, [Event, After, Queue]).

-spec dequeue(queue()) -> {event(), queue()} | empty.
dequeue(Queue0) ->
    case queue:out(Queue0) of
        {empty, _}               -> empty;
        {{value, Event}, Queue1} -> {Event, Queue1}
    end.
