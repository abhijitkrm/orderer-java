# Changelog

## Unreleased

- Event-chain digest (`spec/REPLICATION.md` 13.16): `EventDigest` keeps a rolling hash of each partition's events (an egress plug, off the engine's critical path); `PipelineSm.digest` is SHA-256 over the commands applied, the partition count and those chains, so it costs one pipeline barrier instead of a snapshot of every book (300k resting orders: 2 ms against 255 ms). `StateMachine.deepDigest` is the old full-state hash, used for the client protocol's state hash, so tests still compare complete state. Pipeline snapshots are now `PSM2` (they carry the chains). Matches orderer-rust's chains on all 6 shared vectors (`vectors/replication-digest/`); `EventDigestTest` (5 tests) also pins that a resting-price difference is invisible to the chain until it trades. Mixed Java/Rust clusters pass `kill -9`, authentication and snapshot transfer at the default `--digest-every 64`. `PipelineSm`'s build function now takes the `EventDigest` to register.

- `Consensus`: the replication protocol core (`spec/REPLICATION.md`) as a pure state machine, ported from orderer-rust and proved against the shared conformance vectors (`vectors/replication/`: 28 traces, 28,000 recorded drain points, byte-identical effects and state). `Node.entryAt` is public.
- `io.github.abhijitkrm.orderer.cluster`: the replication runtime, ported from the Rust, C++ and Go ones (`spec/REPLICATION.md` 13.13). WAL and snapshot store (byte-identical to Rust's, against `vectors/replication-wire/`), TCP transport with the pre-shared-key handshake and per-frame MAC, `Replica` (driver, WAL and applier threads, back-pressure, digest-based divergence halt, compaction and snapshot install), `Client`/`ClientServer` with exactly-once sessions and linearizable `barrier()`, `RecorderSm`, `DedupSm`, `PipelineSm` (the matching pipeline as the replicated state machine, same state digest as the other languages), and scale-out (`ShardMap`, `ShardedClient`; group assignment checked against independently computed values).
- Tools `ordernode`, `orderload` (`run`, `status`, `settle`, `verify`) and `clusterbench` (same flags and output as orderer-rust's). 36 new tests (10 wire-fixture, 17 end-to-end, 3 pipeline, 6 scale-out) registered in `scripts/test.sh`; `scripts/interop.sh` runs mixed Java/Rust/C++/Go clusters under `kill -9` of the leader, with authentication and cross-language snapshot transfer.
- `clusterbench` takes `--pmax` (default 20000): Java's price ladder holds one object per tick, about 70 MB per book at the default range, so the benchmark narrows the range to what its workload trades. Cluster nodes keep the default range (the snapshot header, and so the state digest, depend on it).
- Vendored spec and vectors re-pinned to orderer's replication runtime commit (adds `vectors/replication-wire/`).

- `Pipeline.EgressPlacement` (`Builder.egressPlacement`, `Inline`; default `Thread`): the
  engine thread runs the egress plugs itself, with no outbox ring, egress
  thread or per-event copy. Journals must be off (`PipelineError.Kind.Config`
  otherwise). Same events, same order (`PipelineTest`). `orderbench --egress
  thread|inline`. Measured slower with the protocol's instrumentation on (P=2
  17.2M -> 11.9M) and about level with it off; the default is unchanged.
- `orderbench --latency on|off`. `off` stops stamping and sampling commands, which
  the BENCH.md protocol does with two clock reads per command; default `on`.
  Journals-off pipeline, M1: P=2 no-op core 22.7M -> 41.3M, real core P=3
  16.4M -> 20.6M.

## 0.2.1 (orderer-spec/1.3)

- Vendors matcher-java 20e563b: the price ladder finds the next best price through a
  summary bitmap, and an emptied side resets at once. A book whose side
  emptied used to scan the whole ladder per cancel (W3 at 1M ops: 4-24x
  faster in the matcher bench). Output unchanged.
- Repair (orderer-spec 1.3), for three kinds of damage SIGKILL left that
  `--repair` refused: binary tails of all-zero records (an interrupted
  write on macOS can extend a file with zeros, a whole write buffer of
  them) are cut; a segment created at a checkpoint whose header never
  arrived is deleted; and when the last segment holds no records, the
  torn segment before it is repaired too.

## 0.2.0 (orderer-spec/1.2)

- Binary journals are version 2 (CRC-32C per record via `java.util.zip.CRC32C`);
  version 1 still reads.
- `Journal.repairDir` / `orderrecover --repair` truncate a torn final record.
- `Pipeline.checkpoint()` rotates journals onto segments at a clean cut,
  writes the snapshot durably and removes covered segments.
- `Pipeline.stats()` (`Stats`): ring depths, per-partition counts,
  watermarks, fsync timings; `PipelineStats.toPrometheus()`.
- `Builder.checkpointEvery(nanos)`: automatic checkpoints from a background
  thread (stopped first at shutdown).
- `orderrun --checkpoint-every K` and `--durable`; `scripts/test.sh` runs the
  vendored `spec/conformance.sh`.

## 0.1.0

- First release: the full orderer pipeline in Java 17, byte-identical to
  orderer-rust 0.1 (`orderer-spec/1.1`).
- Vendors matcher-java `56455eb` and the orderer spec `41019c6`.
- Harness tools (spec/HARNESS.md) and `orderbench` (spec/BENCH.md 1.1).
