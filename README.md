# orderer-java

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

The Java implementation of [orderer](https://github.com/abhijitkrm/orderer):
an LMAX-Disruptor-style, multi-core order-matching engine around the
[matcher](https://github.com/abhijitkrm/matcher) order book. It needs Java 17+
and nothing else, and implements `orderer-spec/1.3`. It is a port of
[orderer-rust](https://github.com/abhijitkrm/orderer-rust), and
**byte-identical** to it: listings, per-partition journals (JSONL and
binary), snapshots and exit codes.

```
Handle.publish ─▶ ingress ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress plugs
 (any threads)   (multi-     (iseq,  │              (journal +
                  producer)   route) └─▶ …            apply)
```

## Install

`scripts/package.sh` builds `dist/orderer-0.2.1.jar` (plus sources and
javadoc jars) with plain JDK tools; `pom.xml` describes the same artifact
(`io.github.abhijitkrm:orderer:0.2.1`) for Maven. It is not yet on Maven
Central.

## Quick start

```java
Egress.Collect events = Egress.collect(true);
try (Pipeline<Core.FifoCore> p = Pipeline.builder()
        .partitions(2)
        .journal(Journal.Config2.of(dir, Journal.Format.Binary))  // durable: fsync every 1024 records
        .egress(events.factory())  // or acks(...), metrics(...), callback(...), your own Egress
        .build()) {
    Pipeline.Handle h = p.handle();  // one per thread; publish from any thread
    h.publish(7, new Command.New(1, Side.Ask, OType.Limit, 100, 10, Tif.Gtc));
    h.publish(7, new Command.New(2, Side.Bid, OType.Limit, 100, 4, Tif.Gtc));
    p.drain();                                       // applied and delivered
    p.snapshot().write(dir.resolve("books.snap"));   // consistent cut: matcher-snap/1 + .meta
}                                                    // close() = shutdown()
```

The runnable version is `src/test/java/.../Quickstart.java`.

## Plug points

| Seam | Type | Built-ins |
|---|---|---|
| Matching core | `Core.MatchingCore` + `Core.Factory` | `FifoCore` (matcher-java `OrderBook` per symbol), `NoopCore` |
| Egress | `Egress` + `Egress.Factory` (one per partition) | `collect`, `callback`, `acks` (durability-gated), `metrics` |
| Routing | `Routing.PartitionMap` | hash (spec/ROUTING.md) + table overrides |
| Journals | `Journal.Config2`, `Journal.FsyncPolicy` | JSONL or binary, group-commit fsync on I/O threads |
| Waiting | `Pipeline.Waits` / `Disruptor.WaitStrategy` | BusySpin, Yield, Backoff, Blocking |
| Recovery | `Recover.recover(…)`, `readSnapshot`, `restore`, `Journal.repairDir` | snapshot + journals → cores at any P; torn tails repaired |
| Checkpoints | `Pipeline.checkpoint()` | durable snapshot + journal segment rotation; old segments removed |

## Build, test, harness

```bash
scripts/build.sh                    # javac --release 17 → out/
scripts/test.sh                     # every suite below
scripts/build-harness.sh            # → harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}
CHECKED=1 scripts/build-harness.sh  # the same, with assertions on (-ea -esa)
scripts/vendored.sh                 # vendored spec/ + vectors/ untouched
```

The suites:

- `RingTest`: the disruptor protocol, ported from orderer-rust.
- `GoldenTest`: every matcher vector and orderer regression vector through
  the pipeline at P=1 and P=4, in both index modes, plus the routing vectors.
- `PipelineTest`: partitions, recovery, controls, durability,
  backpressure, plugs.
- `spec/conformance.sh` (vendored): every orderer vector through the
  harness tools, byte-exact, checkpoints, repair and version-1 journals included.

Cross-implementation proofs (diffuzz, exhaustive, e2e, snapdiff) run from
the spec repo with this repo checked out next to it.

## Performance

`orderbench` follows spec/BENCH.md. Core mode is matcher-java alone (the
isolated number); pipe mode is the whole engine (the integrated number).
On an Apple M1, W6 (64 symbols, 10M commands), one producer:

| | ops/s |
|---|---:|
| core (matcher-java, untimed) | 10.0M |
| pipeline P=1, journals off | 6.9M |
| pipeline P=4, journals off | 13.3M |
| pipeline P=4, binary journal, fsync/1024 | 10.7M |

The spec repo's `docs/RESULTS.md` has the full rows and the cross-language
comparison. Core rows also report `alloc_b_op`: matcher-java allocates one
event record per event (about 24 bytes per command on W6).

See [`docs/DESIGN.md`](docs/DESIGN.md) for the Java-specific design, and
orderer-rust's DESIGN.md for the architecture.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
