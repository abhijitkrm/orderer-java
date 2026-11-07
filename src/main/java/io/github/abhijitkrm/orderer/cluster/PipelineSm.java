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
import java.util.function.BiFunction;
import io.github.abhijitkrm.orderer.EventDigest;

/// The orderer pipeline as the replicated state machine (port of orderer-rust's
/// pipeline_sm.rs). A committed log entry carries a batch of commands; applying
/// it publishes the batch into the local Pipeline in order, so every replica's
/// partitions see the same ingress order and emit byte-identical events
/// (spec/PIPELINE.md §3). The pipeline runs without journaling: the replicated
/// log is the journal. The state serialised for snapshots and digests is
/// `iseq u64 | partitions u32 | matcher-snap/1 body`, the same bytes the other
/// runtimes produce, so replicas of all languages agree on digests.
///
/// Divergence detection uses an {@link EventDigest}: a rolling hash over the events each partition
/// emits, so {@link #digest} costs one pipeline barrier, not a snapshot of every book (spec/REPLICATION.md
/// 13.16). {@link #deepDigest} is the old full-state hash, computed on demand.
public final class PipelineSm<C extends MatchingCore> implements StateMachine {
    static final byte[] SNAP_MAGIC = {'P', 'S', 'M', '2'};
    static final byte[] DIGEST_TAG = "orderer-evdigest/1".getBytes(StandardCharsets.US_ASCII);

    private final Core.Factory<C> core;
    private final OrderBook.Config book;
    private final PartitionMap map;
    private final BiFunction<Pipeline.Initial<C>, EventDigest, Pipeline<C>> build;
    private final int partitions;
    private EventDigest chains;
    /// Commands applied so far (the pipeline's iseq).
    private long iseq;
    private Pipeline<C> pipe;
    private Pipeline.Handle handle;
    private long badEntries;

    /// build constructs a pipeline, optionally from restored state; it must configure book and map's
    /// partition count, no journaling, and register the EventDigest it is given
    /// ({@code .egress(digest.factory())}).
    public PipelineSm(Core.Factory<C> core, OrderBook.Config book, PartitionMap map,
                      BiFunction<Pipeline.Initial<C>, EventDigest, Pipeline<C>> build) {
        this.core = core; this.book = book; this.map = map; this.build = build;
        this.partitions = map.partitions();
        replace(null, new EventDigest(partitions, null));
    }

    /// A ready-made one with a counting (non-accumulating) egress. The egress
    /// runs inline on the engine threads: it is trivially cheap, and replicas'
    /// pipelines share a machine in tests and benchmarks.
    public static PipelineSm<Core.FifoCore> counting(int partitions, OrderBook.Config book) {
        PartitionMap pm = new PartitionMap(partitions);
        return new PipelineSm<>(Core.FifoCore::new, book, pm, (init, digest) -> {
            Pipeline.Builder<Core.FifoCore> b = Pipeline.builder(Core.FifoCore::new).bookConfig(book).partitionMap(pm)
                    .egress(Egress.metrics(0).factory()).egress(digest.factory()).timestamps(false).egressPlacement(Pipeline.EgressPlacement.Inline);
            if (init != null) b.initial(init);
            return b.build();
        });
    }

    private void replace(Pipeline.Initial<C> init, EventDigest digest) {
        Pipeline<C> p = build.apply(init, digest);
        close();
        chains = digest;
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
        iseq += b.size();
    }

    /// The snapshot: PSM2 | iseq u64 | partitions u32 | chain u64 per partition | matcher-snap/1 body.
    private byte[] encodeSnapshot() {
        pipe.drain();   // egress has seen everything before the snapshot is taken
        Pipeline.Snapshot s = pipe.snapshot();
        byte[] body = s.body().getBytes(StandardCharsets.UTF_8);
        long[] c = chains.values();
        Buf o = new Buf(16 + 8 * c.length + body.length).bytes(SNAP_MAGIC).u64(s.iseq()).u32(s.partitions());
        for (long v : c) o.u64(v);
        return o.bytes(body).toArray();
    }

    @Override public byte[] snapshot(long index) { return encodeSnapshot(); }

    @Override public void restore(long index, byte[] d) {
        int head = 16 + 8 * partitions;
        if (d.length < head || d[0] != 'P' || d[1] != 'S' || d[2] != 'M' || d[3] != '2') throw new RestoreException("not a pipeline snapshot (PSM2)");
        long snapIseq = Buf.u64(d, 4);
        int parts = (int) Buf.u32(d, 12);
        if (parts != partitions) throw new RestoreException("snapshot has " + parts + " partitions, this replica runs " + partitions);
        long[] c = new long[parts];
        for (int i = 0; i < parts; i++) c[i] = Buf.u64(d, 16 + 8 * i);
        Pipeline.Snapshot snap = new Pipeline.Snapshot(new String(d, head, d.length - head, StandardCharsets.UTF_8), snapIseq, parts);
        Recover.Restored<C> r;
        try { r = Recover.restore(core, book, map, snap); }
        catch (RuntimeException e) { throw new RestoreException(e.getMessage()); }
        iseq = snapIseq;
        replace(new Pipeline.Initial<>(r.cores(), snapIseq + 1), new EventDigest(parts, c));
    }

    /// The event-chain digest: O(partitions) after one pipeline barrier.
    @Override public byte[] digest(long index) {
        pipe.drain();
        Buf b = new Buf(DIGEST_TAG.length + 12 + 8 * partitions).bytes(DIGEST_TAG).u64(iseq).u32(partitions);
        for (long v : chains.values()) b.u64(v);
        return RecorderSm.sha256(b.toArray());
    }

    /// The full-state hash: SHA-256 of iseq | partitions | matcher-snap/1 body. Costs a snapshot of every book.
    @Override public byte[] deepDigest(long index) {
        pipe.drain();
        Pipeline.Snapshot s = pipe.snapshot();
        byte[] body = s.body().getBytes(StandardCharsets.UTF_8);
        return RecorderSm.sha256(new Buf(12 + body.length).u64(s.iseq()).u32(s.partitions()).bytes(body).toArray());
    }

    public long badEntries() { return badEntries; }
}
