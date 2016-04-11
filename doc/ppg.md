

# Module ppg #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

Plumtree based Process Group.

<a name="types"></a>

## Data Types ##




### <a name="type-channel">channel()</a> ###


<pre><code>
channel() = pid()
</code></pre>

 A broadcast channel



### <a name="type-hyparview_option">hyparview_option()</a> ###


<pre><code>
hyparview_option() = {active_view_size, pos_integer()} | {passive_view_size, pos_integer()} | {active_random_walk_length, pos_integer()} | {passive_random_walk_length, pos_integer()} | {shuffle_count, pos_integer()} | {shuffle_interval, timeout()}
</code></pre>




### <a name="type-join_option">join_option()</a> ###


<pre><code>
join_option() = {plumtree, [<a href="#type-plumtree_option">plumtree_option()</a>]} | {hyparview, [<a href="#type-hyparview_option">hyparview_option()</a>]}
</code></pre>




### <a name="type-join_options">join_options()</a> ###


<pre><code>
join_options() = [<a href="#type-join_option">join_option()</a>]
</code></pre>




### <a name="type-member">member()</a> ###


<pre><code>
member() = pid()
</code></pre>

 A member process

This process receives messages which are broadcasted to the belonging group



### <a name="type-message">message()</a> ###


<pre><code>
message() = term()
</code></pre>

 A broadcast message



### <a name="type-name">name()</a> ###


<pre><code>
name() = term()
</code></pre>

 Group Name



### <a name="type-plumtree_option">plumtree_option()</a> ###


<pre><code>
plumtree_option() = {gossip_wait_timeout, timeout()} | {ihave_retention_period, timeout()} | {wehave_retention_period, timeout()} | {ticktime, timeout()}
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#broadcast-2">broadcast/2</a></td><td>Broadcasts <code>Message</code> to the group associated with <code>Channel</code></td></tr><tr><td valign="top"><a href="#create-1">create/1</a></td><td>Creates a new group.</td></tr><tr><td valign="top"><a href="#default_join_options-0">default_join_options/0</a></td><td>Returns the default join options.</td></tr><tr><td valign="top"><a href="#delete-1">delete/1</a></td><td>Deletes the group from the local node.</td></tr><tr><td valign="top"><a href="#get_closest_member-1">get_closest_member/1</a></td><td>Returns a process on the local node, if such a process exist.</td></tr><tr><td valign="top"><a href="#get_local_members-1">get_local_members/1</a></td><td>Returns all processes running on the local node in the group <code>Group</code></td></tr><tr><td valign="top"><a href="#get_members-1">get_members/1</a></td><td>Returns all processes  in the group <code>Group</code></td></tr><tr><td valign="top"><a href="#join-2">join/2</a></td><td>Equivalent to <a href="#join-3"><tt>join(Group, Member, default_join_options())</tt></a>.</td></tr><tr><td valign="top"><a href="#join-3">join/3</a></td><td>Joins the process <code>Member</code> to the group <code>Group</code></td></tr><tr><td valign="top"><a href="#leave-1">leave/1</a></td><td>Leaves the group associated with <code>Channel</code></td></tr><tr><td valign="top"><a href="#which_groups-0">which_groups/0</a></td><td>Returns the list of locally known groups.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="broadcast-2"></a>

### broadcast/2 ###

<pre><code>
broadcast(Channel::<a href="#type-channel">channel()</a>, Message::<a href="#type-message">message()</a>) -&gt; ok
</code></pre>
<br />

Broadcasts `Message` to the group associated with `Channel`

<a name="create-1"></a>

### create/1 ###

<pre><code>
create(Group::<a href="#type-name">name()</a>) -&gt; ok
</code></pre>
<br />

Creates a new group

If the group already exists on the node, nothing happens.

Unlike the pg2 module, every nodes which are interested in the group must call this function.

<a name="default_join_options-0"></a>

### default_join_options/0 ###

<pre><code>
default_join_options() -&gt; <a href="#type-join_options">join_options()</a>
</code></pre>
<br />

Returns the default join options

<a name="delete-1"></a>

### delete/1 ###

<pre><code>
delete(Group::<a href="#type-name">name()</a>) -&gt; ok
</code></pre>
<br />

Deletes the group from the local node

<a name="get_closest_member-1"></a>

### get_closest_member/1 ###

<pre><code>
get_closest_member(Group::<a href="#type-name">name()</a>) -&gt; {ok, {<a href="#type-member">member()</a>, <a href="#type-channel">channel()</a>}} | {error, Reason}
</code></pre>

<ul class="definitions"><li><code>Reason = {no_such_group, <a href="#type-name">name()</a>} | {no_reachable_member, <a href="#type-name">name()</a>}</code></li></ul>

Returns a process on the local node, if such a process exist. Otherwise, it returns the contact service process.

<a name="get_local_members-1"></a>

### get_local_members/1 ###

<pre><code>
get_local_members(Group::<a href="#type-name">name()</a>) -&gt; {ok, [{<a href="#type-member">member()</a>, <a href="#type-channel">channel()</a>}]} | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

Returns all processes running on the local node in the group `Group`

<a name="get_members-1"></a>

### get_members/1 ###

<pre><code>
get_members(Group::<a href="#type-name">name()</a>) -&gt; {ok, [{<a href="#type-member">member()</a>, <a href="#type-channel">channel()</a>}]} | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

Returns all processes  in the group `Group`

This function is provided for debugging purposes only.

<a name="join-2"></a>

### join/2 ###

<pre><code>
join(Group::<a href="#type-name">name()</a>, Member::<a href="ppg.md#type-member">ppg:member()</a>) -&gt; {ok, <a href="#type-channel">channel()</a>} | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

Equivalent to [`join(Group, Member, default_join_options())`](#join-3).

<a name="join-3"></a>

### join/3 ###

<pre><code>
join(Group::<a href="#type-name">name()</a>, Member::<a href="ppg.md#type-member">ppg:member()</a>, Options::<a href="#type-join_options">join_options()</a>) -&gt; {ok, Channel::<a href="#type-channel">channel()</a>} | {error, {no_such_group, <a href="#type-name">name()</a>}}
</code></pre>
<br />

Joins the process `Member` to the group `Group`

After the joining, `Member` can broadcast messages via `Channel`.

If you are interested in the disconnection of the channel,
you should apply [`erlang:monitor/2`](erlang.md#monitor-2) or [`erlang:link/1`](erlang.md#link-1) to the channel.

<a name="leave-1"></a>

### leave/1 ###

<pre><code>
leave(Channel::<a href="#type-channel">channel()</a>) -&gt; ok
</code></pre>
<br />

Leaves the group associated with `Channel`

<a name="which_groups-0"></a>

### which_groups/0 ###

<pre><code>
which_groups() -&gt; [<a href="#type-name">name()</a>]
</code></pre>
<br />

Returns the list of locally known groups

