

# Module ppg_debug #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

This module provides debugging functionalities.

Copyright (c) 2016 Takeru Ohta <phjgt308@gmail.com>

<a name="types"></a>

## Data Types ##




### <a name="type-get_graph_option">get_graph_option()</a> ###


<pre><code>
get_graph_option() = {timeout, timeout()} | {format, native | dot} | {include_lazy_edge, boolean()}
</code></pre>




### <a name="type-get_graph_options">get_graph_options()</a> ###


<pre><code>
get_graph_options() = [<a href="#type-get_graph_option">get_graph_option()</a>]
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#broadcast-2">broadcast/2</a></td><td></td></tr><tr><td valign="top"><a href="#get_graph-1">get_graph/1</a></td><td>Equivalent to <a href="#get_graph-2"><tt>get_graph(Group, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#get_graph-2">get_graph/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="broadcast-2"></a>

### broadcast/2 ###

<pre><code>
broadcast(Group::<a href="ppg.md#type-name">ppg:name()</a>, Message::<a href="ppg.md#type-message">ppg:message()</a>) -&gt; ok
</code></pre>
<br />

<a name="get_graph-1"></a>

### get_graph/1 ###

<pre><code>
get_graph(Group::<a href="ppg.md#type-name">ppg:name()</a>) -&gt; <a href="ppg_peer.md#type-graph">ppg_peer:graph()</a>
</code></pre>
<br />

Equivalent to [`get_graph(Group, [])`](#get_graph-2).

<a name="get_graph-2"></a>

### get_graph/2 ###

<pre><code>
get_graph(Group::<a href="ppg.md#type-name">ppg:name()</a>, Options::<a href="#type-get_graph_options">get_graph_options()</a>) -&gt; <a href="ppg_peer.md#type-graph">ppg_peer:graph()</a>
</code></pre>
<br />

