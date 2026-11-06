package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.orderer.Consensus.Entry;
import io.github.abhijitkrm.orderer.Core;
import io.github.abhijitkrm.orderer.Core.MatchingCore;
import io.github.abhijitkrm.orderer.Egress;
import io.github.abhijitkrm.orderer.Pipeline;
import io.github.abhijitkrm.orderer.Recover;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/// The orderer pipeline as the replicated state machine (port of orderer-rust's
/// pipeline_sm.rs). A committed log entry carries a batch of commands; applying
/// it publishes the batch into the local Pipeline in order, so every replica's
/// partitions see the same ingress order and emit byte-identical events
/// (spec/PIPELINE.md §3). The pipeline runs without journaling: the replicated
/// log is the journal. The state serialised for snapshots and digests is
/// `iseq u64 | partitions u32 | matcher-snap/1 body`, the same bytes the other
/// runtimes produce, so replicas of all languages agree on digests.
public final class PipelineSm<C extends MatchingCore> implements StateMachine {
    private final Core.Factory<C> core;
    private final OrderBook.Config book;
    private final PartitionMap map;
    private final Function<Pipeline.Initial<C>, Pipeline<C>> build;
    private Pipeline<C> pipe;
    private Pipeline.Handle handle;
    private long badEntries;

    /// build constructs a pipeline, optionally from restored state; it must
    /// configure book and map's partition count, and no journaling.
    public PipelineSm(Core.Factory<C> core, OrderBook.Config book, PartitionMap map, Function<Pipeline.Initial<C>, Pipeline<C>> build) {
        this.core = core; this.book = book; this.map = map; this.build = build;
        replace(null);
    }

    /// A ready-made one with a counting (non-accumulating) egress. The egress
    /// runs inline on the engine threads: it is trivially cheap, and replicas'
    /// pipelines share a machine in tests and benchmarks.
    public static PipelineSm<Core.FifoCore> counting(int partitions, OrderBook.Config book) {
        PartitionMap pm = new PartitionMap(partitions);
        return new PipelineSm<>(Core.FifoCore::new, book, pm, init -> {
            Pipeline.Builder<Core.FifoCore> b = Pipeline.builder(Core.FifoCore::new).bookConfig(book).partitionMap(pm)
                    .egress(Egress.metrics(0).factory()).timestamps(false).egressPlacement(Pipeline.EgressPlacement.Inline);
            if (init != null) b.initial(init);
            return b.build();
        });
    }

    private void replace(Pipeline.Initial<C> init) {
        Pipeline<C> p = build.apply(init);
        close();
        pipe = p;
        handle = p.handle();
    }

    public void close() {
        if (pipe != null) { handle.close(); pipe.shutdown(); pipe = null; handle = null; }
    }

    @Override public void apply(long index, Entry e) {
        Codec.Batch b;
        try { b = Codec.decodeBatch(e.data()); }
        catch (Codec.CodecException x) { badEntries++; return; }   // every replica skips it identically
        if (b.size() == 0) return;
        if (handle.publishBatch(b.syms(), b.cmds()) != Pipeline.Status.Ok) throw new IllegalStateException("pipeline rejected committed commands");
    }

    private byte[] encodeState() {
        pipe.drain();   // egress has seen everything before the snapshot is taken
        Pipeline.Snapshot s = pipe.snapshot();
        byte[] body = s.body().getBytes(StandardCharsets.UTF_8);
        return new Buf(12 + body.length).u64(s.iseq()).u32(s.partitions()).bytes(body).toArray();
    }

    @Override public byte[] snapshot(long index) { return encodeState(); }

    @Override public void restore(long index, byte[] d) {
        if (d.length < 12) throw new RestoreException("short pipeline snapshot");
        long iseq = Buf.u64(d, 0);
        int partitions = (int) Buf.u32(d, 8);
        Pipeline.Snapshot snap = new Pipeline.Snapshot(new String(d, 12, d.length - 12, StandardCharsets.UTF_8), iseq, partitions);
        Recover.Restored<C> r;
        try { r = Recover.restore(core, book, map, snap); }
        catch (RuntimeException e) { throw new RestoreException(e.getMessage()); }
        replace(new Pipeline.Initial<>(r.cores(), iseq + 1));
    }

    @Override public byte[] digest(long index) { return RecorderSm.sha256(encodeState()); }

    public long badEntries() { return badEntries; }
}
