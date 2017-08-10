ppg
===

[![hex.pm version](https://img.shields.io/hexpm/v/ppg.svg)](https://hex.pm/packages/ppg)

Plumtree based Process Group

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

> {ok, Channel} = ppg:join(foo, self()).
> lists:foreach(fun (_) -> ppg:join(foo, self()) end, lists:seq(1, 4)).

> ppg:broadcast(Channel, bar).
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

- [Plumtree: Epidemic Broadcast Trees](http://www.gsd.inesc-id.pt/~ler/reports/srds07.pdf)
- [HyParView: a membership protocol for reliable gossip-based broadcast](http://www.gsd.inesc-id.pt/~ler/reports/dsn07-leitao.pdf)

License
-------

This library is released under the MIT License.

See the [LICENSE](LICENSE) file for full license information.
