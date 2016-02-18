

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
get_graph_option() = {timeout, timeout()} | {format, native | dot | {png, <a href="#type-graphviz_command">graphviz_command()</a>, <a href="file.md#type-name_all">file:name_all()</a>}} | {edge, eager | lazy | both}
</code></pre>




### <a name="type-get_graph_options">get_graph_options()</a> ###


<pre><code>
get_graph_options() = [<a href="#type-get_graph_option">get_graph_option()</a>]
</code></pre>




### <a name="type-graphviz_command">graphviz_command()</a> ###


<pre><code>
graphviz_command() = dot | neato | twopi | circo | fdp
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#broadcast-2">broadcast/2</a></td><td></td></tr><tr><td valign="top"><a href="#get_graph-1">get_graph/1</a></td><td>Equivalent to <a href="#get_graph-2"><tt>get_graph(Group, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#get_graph-2">get_graph/2</a></td><td></td></tr><tr><td valign="top"><a href="#join_n-2">join_n/2</a></td><td></td></tr><tr><td valign="top"><a href="#leave_n-2">leave_n/2</a></td><td></td></tr><tr><td valign="top"><a href="#reachability_test-5">reachability_test/5</a></td><td></td></tr></table>


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

<a name="join_n-2"></a>

### join_n/2 ###

<pre><code>
join_n(Group::<a href="ppg.md#type-name">ppg:name()</a>, N::non_neg_integer()) -&gt; ok
</code></pre>
<br />

<a name="leave_n-2"></a>

### leave_n/2 ###

<pre><code>
leave_n(Group::<a href="ppg.md#type-name">ppg:name()</a>, N::non_neg_integer()) -&gt; ok
</code></pre>
<br />

<a name="reachability_test-5"></a>

### reachability_test/5 ###

<pre><code>
reachability_test(MessageCount::pos_integer(), BeforeJoin::timeout(), BeforeBroadcast::timeout(), AfterBroadcast::timeout(), Options) -&gt; MissingMessageCount
</code></pre>

<ul class="definitions"><li><code>Options = [{group, <a href="ppg.md#type-name">ppg:name()</a>} | <a href="ppg.md#type-join_option">ppg:join_option()</a>]</code></li><li><code>MissingMessageCount = non_neg_integer()</code></li></ul>

