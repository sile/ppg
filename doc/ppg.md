

# Module ppg #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

Plumtree based Process Group.

Copyright (c) 2016 Takeru Ohta <phjgt308@gmail.com>

<a name="types"></a>

## Data Types ##




### <a name="type-communication_graph">communication_graph()</a> ###


<pre><code>
communication_graph() = [{Node::pid(), Member::pid(), [Edge::{eager | lazy, pid()}]}]
</code></pre>




### <a name="type-join_option">join_option()</a> ###


<pre><code>
join_option() = {contact_process_count, pos_integer()}
</code></pre>




### <a name="type-join_options">join_options()</a> ###


<pre><code>
join_options() = [<a href="#type-join_option">join_option()</a>]
</code></pre>




### <a name="type-name">name()</a> ###


<pre><code>
name() = term()
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#broadcast-2">broadcast/2</a></td><td></td></tr><tr><td valign="top"><a href="#create-1">create/1</a></td><td></td></tr><tr><td valign="top"><a href="#default_join_options-0">default_join_options/0</a></td><td></td></tr><tr><td valign="top"><a href="#delete-1">delete/1</a></td><td></td></tr><tr><td valign="top"><a href="#get_closest_pid-1">get_closest_pid/1</a></td><td></td></tr><tr><td valign="top"><a href="#get_graph-1">get_graph/1</a></td><td></td></tr><tr><td valign="top"><a href="#get_local_members-1">get_local_members/1</a></td><td></td></tr><tr><td valign="top"><a href="#get_members-1">get_members/1</a></td><td></td></tr><tr><td valign="top"><a href="#join-2">join/2</a></td><td></td></tr><tr><td valign="top"><a href="#join-3">join/3</a></td><td></td></tr><tr><td valign="top"><a href="#leave-2">leave/2</a></td><td></td></tr><tr><td valign="top"><a href="#which_groups-0">which_groups/0</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="broadcast-2"></a>

### broadcast/2 ###

<pre><code>
broadcast(Group::<a href="#type-name">name()</a>, Message::term()) -&gt; ok | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

<a name="create-1"></a>

### create/1 ###

<pre><code>
create(Group::<a href="#type-name">name()</a>) -&gt; ok
</code></pre>
<br />

<a name="default_join_options-0"></a>

### default_join_options/0 ###

<pre><code>
default_join_options() -&gt; <a href="#type-join_options">join_options()</a>
</code></pre>
<br />

<a name="delete-1"></a>

### delete/1 ###

<pre><code>
delete(Group::<a href="#type-name">name()</a>) -&gt; ok
</code></pre>
<br />

<a name="get_closest_pid-1"></a>

### get_closest_pid/1 ###

<pre><code>
get_closest_pid(Group::<a href="#type-name">name()</a>) -&gt; pid() | {error, Reason}
</code></pre>

<ul class="definitions"><li><code>Reason = {no_process, <a href="#type-name">name()</a>} | {no_such_group, <a href="#type-name">name()</a>}</code></li></ul>

<a name="get_graph-1"></a>

### get_graph/1 ###

<pre><code>
get_graph(Group::<a href="#type-name">name()</a>) -&gt; <a href="#type-communication_graph">communication_graph()</a> | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

<a name="get_local_members-1"></a>

### get_local_members/1 ###

<pre><code>
get_local_members(Group::<a href="#type-name">name()</a>) -&gt; [pid()] | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

<a name="get_members-1"></a>

### get_members/1 ###

<pre><code>
get_members(Group::<a href="#type-name">name()</a>) -&gt; [pid()] | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

<a name="join-2"></a>

### join/2 ###

<pre><code>
join(Group::<a href="#type-name">name()</a>, Member::pid()) -&gt; ok | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

<a name="join-3"></a>

### join/3 ###

<pre><code>
join(Group::<a href="#type-name">name()</a>, Member::pid(), Options::<a href="#type-join_options">join_options()</a>) -&gt; ok | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

<a name="leave-2"></a>

### leave/2 ###

<pre><code>
leave(Group::<a href="#type-name">name()</a>, Member::pid()) -&gt; ok | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

<a name="which_groups-0"></a>

### which_groups/0 ###

<pre><code>
which_groups() -&gt; [<a href="#type-name">name()</a>]
</code></pre>
<br />

