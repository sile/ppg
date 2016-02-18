%% @copyright 2016 Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc This module provides debugging functionalities
-module(ppg_debug).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([get_graph/1, get_graph/2]).
-export([broadcast/2]).

%% TODO: random test function

-export_type([get_graph_options/0, get_graph_option/0]).
-export_type([graphviz_command/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-type get_graph_options() :: [get_graph_option()].

-type get_graph_option() :: {timeout, timeout()}
                          | {format, native | dot | {png, graphviz_command(), filename:name_all()}}
                          | {edge, eager|lazy|both}.

-type graphviz_command() :: dot | neato | twopi | circo | fdp.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @equiv get_graph(Group, [])
-spec get_graph(ppg:name()) -> ppg_peer:graph().
get_graph(Group) ->
    get_graph(Group, []).

-spec get_graph(ppg:name(), get_graph_options()) -> ppg_peer:graph().
get_graph(Group, Options) ->
    Timeout = proplists:get_value(timeout, Options, 5000),
    Format = proplists:get_value(format, Options, native),
    IncludeEdge = proplists:get_value(edge, Options, both),

    {ok, {_, Peer}} = ppg:get_closest_member(Group),
    Graph0 = ppg_peer:get_graph(Peer, Timeout),
    Graph1 =
        case IncludeEdge of
            both -> Graph0;
            _    -> [{P, M, lists:filter(fun (#{type := Type}) -> Type =:= IncludeEdge end, Edges)} || {P, M, Edges} <- Graph0]
        end,
    Graph2 = lists:sort(Graph1),
    format_graph(Graph2, Group, Format).

-spec broadcast(ppg:name(), ppg:message()) -> ok.
broadcast(Group, Message) ->
    {ok, {_, Peer}} = ppg:get_closest_member(Group),
    ppg:broadcast(Peer, Message).

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec format_graph(ppg_peer:graph(), ppg:name(), native) -> ppg_peer:graph();
                  (ppg_peer:graph(), ppg:name(), dot)    -> binary();
                  (ppg_peer:graph(), ppg:name(), {png, graphviz_command(), filname:name_all()}) -> ok.
format_graph(Graph, _Group, native) ->
    Graph;
format_graph(Graph, Group, {png, Command, OutputPath}) ->
    CommandPath = os:find_executable(atom_to_list(Command)),
    Dot = format_graph(Graph, Group, dot),
    _ = spawn_link(
          fun () ->
                  Port = open_port({spawn_executable, CommandPath},
                                [{args, ["-T", "png", "-o", list_to_binary(OutputPath)]},
                                 use_stdio, exit_status]),
                  port_command(Port, Dot)
          end),
    ok;
format_graph(Graph, Group, dot) ->
    _ = case ppg_contact_service:find_peer(ppg_contact_service:new(Group)) of
            error             -> ContactPeer = self(); % NOTE: dummy value
            {ok, ContactPeer} -> ok
        end,
    GraphName = http_uri:encode(lists:flatten(io_lib:format("~w", [Group]))),
    list_to_binary(
      [
       "graph ", GraphName, " {\n",
       generate_dot_nodes(Graph, ContactPeer),
       generate_dot_edges(Graph),
       "}\n"
      ]).

-spec generate_dot_nodes(ppg_peer:graph(), ppg_peer:peer()) -> iodata().
generate_dot_nodes([], _) ->
    [];
generate_dot_nodes([{Peer, Member, _} | Graph], ContactPeer) ->
    Color =
        case Peer =:= ContactPeer of
            true  -> yellow;
            false -> white
        end,
    [io_lib:format("  \"~p\" [shape=circle,label=\"~p\\n~p\\n~s\",style=filled,fillcolor=~s];\n",
                   [Peer, Member, Peer, node(Member), Color]),
     generate_dot_nodes(Graph, ContactPeer)].

-spec generate_dot_edges(ppg_peer:graph()) -> iodata().
generate_dot_edges([]) ->
    [];
generate_dot_edges([{From, _, Edges} | Graph]) ->
    [[begin
          Color = case Type of eager -> red; lazy -> blue end,
          io_lib:format("  \"~p\" -- \"~p\" [color=~s,penwidth=~p];\n", [From, To, Color, Weight+1])
      end || #{pid := To, type := Type, nohaves := Weight} <- Edges, From < To],
     generate_dot_edges(Graph)].
