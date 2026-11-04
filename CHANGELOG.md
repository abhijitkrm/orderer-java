# Changelog

## Unreleased

- `Consensus`: the replication protocol core (`spec/REPLICATION.md`) as a pure state machine, ported from orderer-rust and proved against the shared conformance vectors (`vectors/replication/`: 28 traces, 28,000 recorded drain points, byte-identical effects and state). Only the protocol core is ported; the WAL, transport and replica runtime are not.

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
