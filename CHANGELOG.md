# Changelog

## Unreleased: orderer-spec/1.2

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
