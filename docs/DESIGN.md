# orderer-java design

The architecture is orderer-rust's (its `docs/DESIGN.md`); this file covers
what is specific to Java. The port follows orderer-rust's porting checklist
(§8) and orderer-cpp's layout file for file.

| orderer-rust | orderer-java |
|---|---|
| orderer-disruptor | `Disruptor.java` |
| orderer-core (core.rs, snapshot) | `Core.java`, `Flat.java` |
| routing.rs | `Routing.java` |
| journal.rs + writer.rs | `Journal.java` (`ChunkWriter` inside) |
| msg.rs + egress.rs | `Egress.java` |
| pipeline.rs | `Pipeline.java` |
| recover.rs | `Recover.java` |
| harness.rs + bins | `Harness.java` + `tools/*.java` |

## Memory model

- `Sequence` is a padded `volatile long` read with `getAcquire` and written
  with `setRelease` through a `VarHandle`, exactly the orderings the Rust
  ring uses. Claims use `getAndAdd`; `tryPublish` uses `compareAndSet` so a
  failed attempt never leaks a claim.
- Multi-producer availability is an `AtomicIntegerArray` of lap numbers
  (`setRelease` / `getAcquire`).
- The shutdown handshake (per-handle in-flight flag, then the closed flag)
  needs store→load ordering. Both are plain `volatile` fields, and Java
  volatiles are sequentially consistent, which is what the Rust code gets
  from `SeqCst`.

## Slots and allocation

Ring slots are mutable objects (`CmdMsg`, `EvtMsg`) created once by the
ring's factory and rewritten in place, so the rings never allocate. The
engine thread is its own `Core.Emit` and `FifoCore` reuses one tagging
sink, so the hot path adds no lambdas or boxing. What remains is
matcher-java's: `Command` and `Event` are records, so each event is one
small allocation. `orderbench --mode core` reports it as `alloc_b_op`
instead of claiming zero; the young-generation collector absorbs it.

## Journals

`ChunkWriter` encodes records into 64 preallocated 256 KB direct buffers.
Full chunks, or idle ones after 50 µs, go to the file's I/O thread over an
`ArrayBlockingQueue`. The I/O thread writes everything queued, then decides
on one `FileChannel.force(false)` (group commit), then publishes the
`flushed` and `durable` watermarks. `acks` releases events only up to
`durable`. On macOS the JDK, not orderer, decides whether `force` is
`fsync` or `F_FULLFSYNC`; orderer-rust and orderer-cpp always use
`F_FULLFSYNC`, so durable rows from macOS are not strictly comparable.

## Harness tools

`scripts/build-harness.sh` writes shell wrappers into `harness/bin/`. The
four conformance tools run with C1 only and the serial collector, because
JVM startup dominates short runs. `orderbench` gets the full JIT, the
parallel collector and a pre-touched heap. Core and pipe modes run the
spec's warmup three times (`--warmups N`) so the timed pass measures
compiled code.
