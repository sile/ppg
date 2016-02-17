ppg
===

Plumtree based process group

Build
-----

```sh
$ git clone git@github.com:sile/ppg.git
$ cd ppg/
$ ./rebar3 compile
```

Usage Examples
--------------

```erlang
$ make start
> ppg:create(foo).

> ppg:which_groups().
[foo].

> {ok, Peer} = ppg:join(foo, self()).
> lists:foreach(fun (_) -> ppg:join(foo, self()) end, lists:seq(1, 4)).

> ppg:broadcast(Peer, bar).
> flush().
Shell got bar
Shell got bar
Shell got bar
Shell got bar
Shell got bar
ok
```

API
---

See [EDoc Documents](doc/README.md)

Reference
----------

- [Plumtree: Epidemic Broadcast Trees](http://homepages.gsd.inesc-id.pt/~jleitao/pdf/srds07-leitao.pdf)
- [HyParView: a membership protocol for reliable gossip-based broadcast](http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf)

