//! Rings, threads and control (spec/PIPELINE.md).
//!
//!   Handle.publish ─▶ ingress (multi-producer) ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress
//!                                                         └─▶ …            (journal + apply)
//!
//! router (1): sole ingress consumer; stamps iseq, routes, broadcasts controls,
//!   commits every inbox once per batch.
//! engine[p]: encodes each command's journal record into its partition's
//!   ChunkWriter, then applies it (journal-before-apply, in-thread), staging
//!   events into outbox[p].
//! egress (grouped): runs the plugs, marks drain epochs.
//! I/O threads (one per journal file, inside ChunkWriter): write + fsync.
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.orderer.Core.Block;
import io.github.abhijitkrm.orderer.Core.MatchingCore;
import io.github.abhijitkrm.orderer.Disruptor.Consumer;
import io.github.abhijitkrm.orderer.Disruptor.MultiProducer;
import io.github.abhijitkrm.orderer.Disruptor.Publish;
import io.github.abhijitkrm.orderer.Disruptor.RingBuilder;
import io.github.abhijitkrm.orderer.Disruptor.RingControl;
import io.github.abhijitkrm.orderer.Disruptor.SingleProducer;
import io.github.abhijitkrm.orderer.Disruptor.WaitStrategy;
import io.github.abhijitkrm.orderer.Egress.CmdMsg;
import io.github.abhijitkrm.orderer.Egress.Control;
import io.github.abhijitkrm.orderer.Egress.EvtMsg;
import io.github.abhijitkrm.orderer.Journal.ChunkWriter;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

public final class Pipeline<C extends MatchingCore> implements AutoCloseable {

    public static final class PipelineError extends RuntimeException {
        public enum Kind { Closed, Full, Failed, Config, Io }
        public final Kind kind;
        public PipelineError(Kind k, String m) { super(m); kind = k; }
    }

    /// Publish outcome: Ok means sequenced and will be applied — not durable.
    public enum Status { Ok, Closed, Full }

    /// Wait strategy per stage (not observable).
    public record Waits(WaitStrategy router, WaitStrategy engine, WaitStrategy egress) {
        public static Waits relaxed() { return new Waits(WaitStrategy.backoff(), WaitStrategy.backoff(), WaitStrategy.backoff()); }
        /// Router and engines busy-spin; egress backs off. The bench configuration.
        public static Waits lowLatency() { return new Waits(WaitStrategy.busySpin(), WaitStrategy.busySpin(), WaitStrategy.backoff()); }
    }

    /// Starting state after recovery.
    public record Initial<C>(List<C> cores, long nextIseq) {}

    /// A merged matcher-snap/1 snapshot plus its cut (spec/JOURNAL.md §4).
    public record Snapshot(String body, long iseq, int partitions) {
        public String meta() {
            return "{\"format\":\"orderer-meta/1\",\"iseq\":" + Long.toUnsignedString(iseq) + ",\"partitions\":" + partitions + "}\n";
        }
        public void write(Path path) throws IOException {
            Files.writeString(path, body, StandardCharsets.UTF_8);
            Files.writeString(metaPath(path), meta(), StandardCharsets.UTF_8);
        }
    }

    public static Path metaPath(Path p) { return Path.of(p.toString() + ".meta"); }

    // ---- shared state ----------------------------------------------------------------------------

    static final class InFlight { volatile boolean v; }

    static final class SnapState {
        final List<Block> blocks = new ArrayList<>();
        int remaining;
        long cut;
    }

    static final class Shared {
        int partitions;
        Config book;
        final long epoch = System.nanoTime();
        volatile boolean timestamps, closed, failed;
        String failure;
        final CopyOnWriteArrayList<InFlight> handles = new CopyOnWriteArrayList<>();
        final AtomicLong nextEpoch = new AtomicLong(), nextOp = new AtomicLong();
        Disruptor.Sequence[] egressEpoch;
        final Map<Long, SnapState> snaps = new HashMap<>();  // guarded by itself
        final List<AtomicLong> flushed = new ArrayList<>(), durable = new ArrayList<>();
        final CopyOnWriteArrayList<Runnable> alerts = new CopyOnWriteArrayList<>();
        Journal.Config2 journal;
        final List<Stats.EngineCounters> counters = new ArrayList<>();
        final List<Stats.IoStats> io = new ArrayList<>();
        RingControl<CmdMsg> ingressCtl;
        final List<RingControl<CmdMsg>> inboxCtl = new ArrayList<>();
        final List<RingControl<EvtMsg>> outboxCtl = new ArrayList<>();

        void alertAll() { for (Runnable a : alerts) a.run(); }
        void fail(String m) {
            synchronized (this) { if (failure == null) failure = m; }
            failed = true;
            alertAll();
            synchronized (snaps) { snaps.notifyAll(); }
        }
        void check() {
            if (failed) {
                synchronized (this) { throw new PipelineError(PipelineError.Kind.Failed, "pipeline failed: " + failure); }
            }
        }
        long nowNanos() { return timestamps ? Math.max(System.nanoTime() - epoch, 1) : 0; }
    }

    /// spin → yield → short sleeps until done, failing fast.
    static void waitUntil(Shared sh, BooleanSupplier done) {
        int step = 0;
        while (!done.getAsBoolean()) {
            sh.check();
            step++;
            if (step < 64) Thread.onSpinWait();
            else if (step < 256) Thread.yield();
            else LockSupport.parkNanos(50_000);
        }
    }

    // ---- Handle ----------------------------------------------------------------------------------

    /// Publishes commands; thread-safe. Each handle registers an in-flight flag
    /// so shutdown can wait out publishes that began before it closed the
    /// pipeline: a publish that returns Ok is always applied. close() unregisters.
    public static final class Handle implements AutoCloseable {
        private final MultiProducer<CmdMsg> ingress;
        private final Shared sh;
        private final InFlight flag = new InFlight();

        Handle(MultiProducer<CmdMsg> ingress, Shared sh) {
            this.ingress = ingress;
            this.sh = sh;
            sh.handles.add(flag);
        }

        private boolean enter() {
            flag.v = true;  // volatile write then volatile read: sequentially consistent
            if (sh.closed) { flag.v = false; return false; }
            return true;
        }
        private void exit() { flag.v = false; }

        static void fill(CmdMsg m, long sym, Command cmd, long t) {
            m.symbol = sym; m.tPub = t; m.ctl = Control.None; m.arg = 0; m.cmd = cmd;
        }

        /// Sequence one command (blocks while ingress is full).
        public Status publish(long sym, Command cmd) {
            if (!enter()) return Status.Closed;
            long t = sh.nowNanos();
            Publish r = ingress.publish(m -> fill(m, sym, cmd, t));
            exit();
            return r == Publish.Ok ? Status.Ok : Status.Closed;
        }

        /// Sequence one command, or Full without waiting.
        public Status tryPublish(long sym, Command cmd) {
            if (!enter()) return Status.Closed;
            long t = sh.nowNanos();
            Publish r = ingress.tryPublish(m -> fill(m, sym, cmd, t));
            exit();
            if (r == Publish.Ok) return Status.Ok;
            return r == Publish.Full ? Status.Full : Status.Closed;
        }

        /// Many commands, one claim per chunk (consecutive iseqs within a chunk).
        public Status publishBatch(long[] syms, Command[] cmds, int from, int n) {
            if (!enter()) return Status.Closed;
            int chunk = Math.min(ingress.size(), 256);
            Publish r = Publish.Ok;
            for (int off = from; off < from + n && r == Publish.Ok; off += chunk) {
                int k = Math.min(chunk, from + n - off), base = off;
                long t = sh.nowNanos();
                r = ingress.publishBatch(k, (i, m) -> fill(m, syms[base + i], cmds[base + i], t));
            }
            exit();
            return r == Publish.Ok ? Status.Ok : Status.Closed;
        }
        public Status publishBatch(long[] syms, Command[] cmds) { return publishBatch(syms, cmds, 0, cmds.length); }

        public void close() { sh.handles.remove(flag); }
    }

    // ---- Builder ---------------------------------------------------------------------------------

    public static final class Builder<C extends MatchingCore> {
        private final Core.Factory<C> factory;
        private Config book = Config.defaults();
        private int partitions = 1;
        private PartitionMap map;
        // tuned in orderer-rust phase 6: L2-friendly, ~4x lower queueing latency
        private int ingressSize = 1 << 14, inboxSize = 1 << 12, outboxSize = 1 << 13;
        private Waits waits = Waits.relaxed();
        private Journal.Config2 journal;
        private final List<Egress.Factory> egress = new ArrayList<>();
        private boolean timestamps;
        private int egressThreads = 1;
        private Initial<C> initial;
        private long checkpointEvery;

        Builder(Core.Factory<C> f) { factory = f; }
        public Builder<C> bookConfig(Config c) { book = c; return this; }
        public Builder<C> partitions(int p) { partitions = p; map = null; return this; }
        public Builder<C> partitionMap(PartitionMap m) { partitions = m.partitions(); map = m; return this; }
        public Builder<C> ringSizes(int ingress, int inbox, int outbox) {
            ingressSize = ingress; inboxSize = inbox; outboxSize = outbox; return this;
        }
        public Builder<C> waits(Waits w) { waits = w; return this; }
        public Builder<C> journal(Journal.Config2 j) { journal = j; return this; }
        public Builder<C> egress(Egress.Factory f) { egress.add(f); return this; }
        public Builder<C> timestamps(boolean on) { timestamps = on; return this; }
        /// Threads running the partitions' egress plugs (partition p → p % n).
        public Builder<C> egressThreads(int n) { egressThreads = Math.max(n, 1); return this; }
        public Builder<C> initial(Initial<C> i) { initial = i; return this; }
        /// Take a checkpoint (spec/JOURNAL.md §6) every `intervalNanos` from a
        /// background thread. Needs journals; shutdown stops the thread first;
        /// a failed checkpoint fails the pipeline.
        public Builder<C> checkpointEvery(long intervalNanos) { checkpointEvery = intervalNanos; return this; }
        public Pipeline<C> build() { return new Pipeline<>(this); }
    }

    public static <C extends MatchingCore> Builder<C> builder(Core.Factory<C> f) { return new Builder<>(f); }
    public static Builder<Core.FifoCore> builder() { return new Builder<>(Core.FifoCore::new); }

    // ---- the pipeline ----------------------------------------------------------------------------

    private final Shared sh;
    private final PartitionMap map;
    private final List<Thread> threads = new ArrayList<>();
    private final Handle handle;
    private final MultiProducer<CmdMsg> ingress;
    private boolean shut;
    private Thread checkpointer;
    private final Object ckptLock = new Object();
    private boolean ckptStop;

    public Handle handle() { return new Handle(ingress, sh); }
    public Status publish(long s, Command c) { return handle.publish(s, c); }
    public Status tryPublish(long s, Command c) { return handle.tryPublish(s, c); }
    public Status publishBatch(long[] syms, Command[] cmds) { return handle.publishBatch(syms, cmds); }
    public int partitions() { return sh.partitions; }
    public int partitionOf(long s) { return map.partition(s); }
    public Config bookConfig() { return sh.book; }
    public void setTimestamps(boolean on) { sh.timestamps = on; }
    public long durableIseq(int p) { return sh.durable.get(p).get(); }

    /// Operational statistics (see Stats).
    public Stats.PipelineStats stats() {
        List<Stats.PartitionStats> ps = new ArrayList<>();
        for (int p = 0; p < sh.partitions; p++) {
            Stats.EngineCounters c = sh.counters.get(p);
            Stats.IoStats io = sh.io.get(p);
            ps.add(new Stats.PartitionStats(p, depth(sh.inboxCtl.get(p).published(), sh.inboxCtl.get(p).consumed()),
                    depth(sh.outboxCtl.get(p).published(), sh.outboxCtl.get(p).consumed()), c.commands, c.events,
                    sh.journal != null ? sh.flushed.get(p).get() : -1L, sh.durable.get(p).get(),
                    io.fsyncs.get(), io.fsyncNsTotal.get(), io.fsyncNsMax.get()));
        }
        return new Stats.PipelineStats(depth(sh.ingressCtl.published(), sh.ingressCtl.consumed()), ps);
    }

    private static long depth(long published, long consumed) { return Math.max(published - consumed, 0); }

    /// Barrier: returns once every command published before the call has been
    /// applied and delivered to every egress plug.
    public void drain() {
        long epoch = sh.nextEpoch.incrementAndGet();
        publishCtl(Control.Barrier, epoch);
        waitUntil(sh, () -> {
            for (int p = 0; p < sh.partitions; p++) if (sh.egressEpoch[p].get() < epoch) return false;
            return true;
        });
    }

    /// A consistent snapshot of every book, cut at this point of the ingress order.
    public Snapshot snapshot() { return snapshotOp(Control.Snapshot); }

    /// A checkpoint (spec/JOURNAL.md §6): a snapshot cut here; every journal
    /// rotates onto a new segment at the cut; the snapshot is written durably
    /// into the journal directory; older segments and checkpoints are removed.
    public Snapshot checkpoint() {
        Journal.Config2 cfg = sh.journal;
        if (cfg == null) throw new PipelineError(PipelineError.Kind.Config, "checkpoint needs journals");
        Snapshot s = snapshotOp(Control.Checkpoint);
        drain();  // every egress has rotated its event journal
        try {
            Path path = Journal.checkpointPath(cfg.dir(), s.iseq());
            Journal.writeDurably(path, s.body().getBytes(StandardCharsets.UTF_8));
            Journal.writeDurably(metaPath(path), s.meta().getBytes(StandardCharsets.UTF_8));
            Journal.removeSegmentsBelow(cfg.dir(), cfg.format(), s.iseq());
            Journal.removeCheckpointsBelow(cfg.dir(), s.iseq());
        } catch (IOException e) {
            throw new PipelineError(PipelineError.Kind.Io, String.valueOf(e.getMessage()));
        }
        return s;
    }

    private Snapshot snapshotOp(Control ctl) {
        long op = sh.nextOp.incrementAndGet();
        synchronized (sh.snaps) {
            SnapState st = new SnapState();
            st.remaining = sh.partitions;
            sh.snaps.put(op, st);
        }
        publishCtl(ctl, op);
        SnapState st;
        synchronized (sh.snaps) {
            for (;;) {
                if (sh.failed) break;
                if (sh.snaps.get(op).remaining == 0) break;
                try { sh.snaps.wait(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            st = sh.snaps.remove(op);
        }
        sh.check();
        st.blocks.sort(Comparator.comparingLong(Block::symbol));
        StringBuilder body = new StringBuilder(Core.header(sh.book));
        for (Block b : st.blocks) body.append(b.text());
        return new Snapshot(body.toString(), st.cut, sh.partitions);
    }

    /// Stop accepting commands, drain everything sequenced, stop every
    /// thread. Idempotent. Throws PipelineError(Failed) if any thread failed.
    public void shutdown() {
        if (shut) { sh.check(); return; }
        shut = true;
        if (checkpointer != null) {  // a checkpoint in progress finishes first
            synchronized (ckptLock) { ckptStop = true; ckptLock.notifyAll(); }
            for (;;) {
                try { checkpointer.join(); break; } catch (InterruptedException ignored) {}
            }
        }
        sh.closed = true;
        List<InFlight> flags = new ArrayList<>(sh.handles);
        try {
            waitUntil(sh, () -> {
                for (InFlight f : flags) if (f.v) return false;
                return true;
            });
        } catch (PipelineError ignored) {}
        if (!sh.failed) {
            Publish r = ingress.publish(m -> { m.symbol = 0; m.tPub = 0; m.arg = 0; m.ctl = Control.Shutdown; m.cmd = null; });
            if (r != Publish.Ok) sh.fail("ingress alerted before shutdown");
        }
        for (Thread t : threads) {
            for (;;) {
                try { t.join(); break; } catch (InterruptedException ignored) {}
            }
        }
        threads.clear();
        sh.alertAll();
        sh.check();
    }

    public void close() { shutdown(); }

    private void publishCtl(Control c, long arg) {
        sh.check();
        if (sh.closed) throw new PipelineError(PipelineError.Kind.Closed, "pipeline closed");
        Publish r = ingress.publish(m -> { m.symbol = 0; m.tPub = 0; m.arg = arg; m.ctl = c; m.cmd = null; });
        if (r != Publish.Ok) throw new PipelineError(PipelineError.Kind.Closed, "pipeline closed");
    }

    static final class EgressPart {
        int p;
        Consumer<EvtMsg> outbox;
        final List<Egress> plugs = new ArrayList<>();
        Egress.Ctx ctx;
        long lastIseq;
        boolean stopped;
    }

    /// Opens a partition's next journal segment (spec/JOURNAL.md §6 step 2).
    record Segmenter(Path dir, Journal.Format format, Journal.Kind kind, int p, int partitions, Config book) {
        void rotate(ChunkWriter w, long cut) {
            try { w.rotate(Journal.openSegment(dir, format, kind, p, partitions, book, cut)); }
            catch (IOException e) { throw new IllegalStateException("journal rotate: " + e.getMessage(), e); }
        }
    }

    /// The event journal as the first plug of its partition.
    static final class EvtJournal implements Egress {
        final ChunkWriter w;
        final Journal.Format f;
        final Segmenter seg;
        long lastHandoff = System.nanoTime();
        EvtJournal(ChunkWriter w, Journal.Format f, Segmenter seg) { this.w = w; this.f = f; this.seg = seg; }
        public void onCheckpoint(long cut) { seg.rotate(w, cut); }
        public void onEvent(EvtMsg m) { w.pushEvt(f, m.seq, m.symbol, m.ev); }
        public void onIdle() {
            if (w.pending() > 0 && System.nanoTime() - lastHandoff >= 50_000) {
                w.handOff();
                lastHandoff = System.nanoTime();
            }
        }
        public void onShutdown() {
            String e = w.finish();
            if (e != null) throw new RuntimeException(e);
        }
    }

    private static boolean pow2(int n) { return n >= 2 && Integer.bitCount(n) == 1; }

    private Pipeline(Builder<C> b) {
        if (b.checkpointEvery > 0 && b.journal == null)
            throw new PipelineError(PipelineError.Kind.Config, "checkpointEvery needs journals");
        PartitionMap m;
        try { m = b.map != null ? b.map : new PartitionMap(b.partitions); }
        catch (Routing.RoutingError e) { throw new PipelineError(PipelineError.Kind.Config, e.getMessage()); }
        map = m;
        final int P = map.partitions();
        if (!pow2(b.ingressSize) || !pow2(b.inboxSize) || !pow2(b.outboxSize))
            throw new PipelineError(PipelineError.Kind.Config, "ring sizes must be powers of two >= 2");
        List<C> cores = new ArrayList<>();
        long nextIseq = 1;
        if (b.initial != null) {
            if (b.initial.cores().size() != P) throw new PipelineError(PipelineError.Kind.Config, "recovered cores != partitions");
            cores.addAll(b.initial.cores());
            nextIseq = Math.max(b.initial.nextIseq(), 1);
        } else {
            for (int p = 0; p < P; p++) cores.add(b.factory.create(b.book));
        }
        final boolean journaled = b.journal != null;
        final long startWm = nextIseq - 1;
        sh = new Shared();
        sh.partitions = P;
        sh.book = b.book;
        sh.timestamps = b.timestamps;
        sh.journal = b.journal;
        sh.egressEpoch = new Disruptor.Sequence[P];
        for (int p = 0; p < P; p++) {
            sh.egressEpoch[p] = new Disruptor.Sequence();
            sh.egressEpoch[p].set(0);
            sh.flushed.add(new AtomicLong(startWm));
            sh.durable.add(new AtomicLong(journaled ? startWm : -1L));
            sh.counters.add(new Stats.EngineCounters());
            sh.io.add(new Stats.IoStats());
        }

        // journals first, so I/O errors surface from build()
        ChunkWriter[] cmdW = new ChunkWriter[P], evtW = new ChunkWriter[P];
        if (journaled) {
            Journal.Config2 jc = b.journal;
            try {
                Files.createDirectories(jc.dir());
                if (!jc.append()) Journal.clearDir(jc.dir(), jc.format());
                for (int p = 0; p < P; p++) {
                    cmdW[p] = new ChunkWriter(Journal.open(jc, Journal.Kind.Cmd, p, P, b.book), jc.fsync(),
                            sh.flushed.get(p), sh.durable.get(p), "orderer-cmd-io-" + p, sh.io.get(p));
                    if (jc.events())
                        evtW[p] = new ChunkWriter(Journal.open(jc, Journal.Kind.Evt, p, P, b.book), null,
                                new AtomicLong(), new AtomicLong(), "orderer-evt-io-" + p);
                }
            } catch (IOException | Journal.CorruptJournal e) {
                for (ChunkWriter w : cmdW) if (w != null) w.finish();
                for (ChunkWriter w : evtW) if (w != null) w.finish();
                throw new PipelineError(PipelineError.Kind.Io, e.getMessage());
            }
        }

        List<SingleProducer<CmdMsg>> inboxes = new ArrayList<>();
        int nEgress = Math.min(b.egressThreads, P);
        List<List<EgressPart>> groups = new ArrayList<>();
        for (int i = 0; i < nEgress; i++) groups.add(new ArrayList<>());
        for (int p = 0; p < P; p++) {
            RingBuilder<CmdMsg> ib = new RingBuilder<>(b.inboxSize, CmdMsg::new);
            ib.consumer(b.waits.engine());
            var inbox = ib.buildSingle();
            RingControl<CmdMsg> ictl = inbox.producer().control();
            sh.inboxCtl.add(ictl);
            sh.alerts.add(ictl::alert);
            inboxes.add(inbox.producer());

            RingBuilder<EvtMsg> ob = new RingBuilder<>(b.outboxSize, EvtMsg::new);
            ob.consumer(b.waits.egress());
            var outbox = ob.buildSingle();
            RingControl<EvtMsg> octl = outbox.producer().control();
            sh.outboxCtl.add(octl);
            sh.alerts.add(octl::alert);

            Engine<C> eng = new Engine<>(sh, sh.counters.get(p), inbox.consumers().get(0), outbox.producer(), cores.get(p), cmdW[p],
                    journaled ? b.journal.format() : null,
                    journaled ? new Segmenter(b.journal.dir(), b.journal.format(), Journal.Kind.Cmd, p, P, b.book) : null);
            threads.add(thread("orderer-engine-" + p, () -> guarded(sh, "engine", eng::run)));

            EgressPart part = new EgressPart();
            part.p = p;
            part.outbox = outbox.consumers().get(0);
            part.ctx = new Egress.Ctx(p, P, sh.epoch, sh.durable.get(p));
            if (evtW[p] != null)
                part.plugs.add(new EvtJournal(evtW[p], b.journal.format(),
                        new Segmenter(b.journal.dir(), b.journal.format(), Journal.Kind.Evt, p, P, b.book)));
            for (Egress.Factory f : b.egress) part.plugs.add(f.create(part.ctx));
            groups.get(p % nEgress).add(part);
        }
        for (int i = 0; i < groups.size(); i++) {
            List<EgressPart> g = groups.get(i);
            threads.add(thread("orderer-egress-" + i, () -> guarded(sh, "egress", () -> egressLoop(sh, g, journaled))));
        }

        RingBuilder<CmdMsg> rb = new RingBuilder<>(b.ingressSize, CmdMsg::new);
        rb.consumer(b.waits.router());
        var ing = rb.buildMulti();
        RingControl<CmdMsg> gctl = ing.producer().control();
        sh.ingressCtl = gctl;
        sh.alerts.add(gctl::alert);
        ingress = ing.producer();
        Consumer<CmdMsg> rcons = ing.consumers().get(0);
        long ni = nextIseq;
        threads.add(thread("orderer-router", () -> guarded(sh, "router", () -> routerLoop(rcons, inboxes, map, ni))));
        handle = new Handle(ingress, sh);
        for (Thread t : threads) t.start();
        if (b.checkpointEvery > 0) {
            long interval = b.checkpointEvery;
            checkpointer = thread("orderer-checkpoint", () -> {
                for (;;) {
                    synchronized (ckptLock) {
                        long deadline = System.nanoTime() + interval;
                        for (long left = interval; !ckptStop && left > 0; left = deadline - System.nanoTime()) {
                            try { ckptLock.wait(Math.max(left / 1_000_000, 1)); } catch (InterruptedException ignored) {}
                        }
                        if (ckptStop || sh.failed) return;
                    }
                    try {
                        checkpoint();
                    } catch (PipelineError e) {
                        if (e.kind != PipelineError.Kind.Closed && e.kind != PipelineError.Kind.Failed)
                            sh.fail("checkpoint: " + e.getMessage());
                        return;
                    } catch (RuntimeException e) {
                        sh.fail("checkpoint: " + e);
                        return;
                    }
                }
            });
            checkpointer.start();
        }
    }

    private static Thread thread(String name, Runnable r) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    private static void guarded(Shared sh, String name, Runnable body) {
        try { body.run(); }
        catch (Throwable e) { sh.fail(name + " thread failed: " + e); }
    }

    private static void routerLoop(Consumer<CmdMsg> ingress, List<SingleProducer<CmdMsg>> inboxes, PartitionMap map,
                                   long nextIseq) {
        final long[] iseq = {nextIseq - 1};
        final boolean[] stop = {false};
        @SuppressWarnings("unchecked")
        SingleProducer<CmdMsg>[] ibs = inboxes.toArray(new SingleProducer[0]);
        Disruptor.Handler<CmdMsg> h = (m, seq, eob) -> {
            if (m.ctl == Control.None) {
                CmdMsg s = ibs[map.partition(m.symbol)].stage();
                if (s != null) { s.copyFrom(m); s.iseq = ++iseq[0]; }
            } else {
                for (SingleProducer<CmdMsg> ib : ibs) {
                    CmdMsg s = ib.stage();
                    if (s != null) { s.copyFrom(m); s.iseq = iseq[0]; }
                }
                if (m.ctl == Control.Shutdown) stop[0] = true;
            }
            if (eob) for (SingleProducer<CmdMsg> ib : ibs) ib.commit();
        };
        while (!stop[0]) {
            if (!ingress.waitPoll(h)) break;
        }
        for (SingleProducer<CmdMsg> ib : ibs) ib.commit();
    }

    /// One partition's engine: journal-before-apply, then stage events.
    static final class Engine<C extends MatchingCore> implements Core.Emit, Disruptor.Handler<CmdMsg> {
        final Shared sh;
        final Consumer<CmdMsg> inbox;
        final SingleProducer<EvtMsg> out;
        final C core;
        final ChunkWriter journal;
        final Journal.Format fmt;
        final Segmenter seg;
        long iseq, tPub;
        boolean stop, force;

        final Stats.EngineCounters counters;
        long nCommands, nEvents;

        Engine(Shared sh, Stats.EngineCounters counters, Consumer<CmdMsg> inbox, SingleProducer<EvtMsg> out, C core,
               ChunkWriter journal, Journal.Format fmt, Segmenter seg) {
            this.counters = counters;
            this.sh = sh; this.inbox = inbox; this.out = out; this.core = core; this.journal = journal; this.fmt = fmt;
            this.seg = seg;
        }

        public void onEvent(long sym, long seq, Event ev) {
            nEvents++;
            EvtMsg e = out.stage();
            if (e == null) throw new IllegalStateException("outbox alerted");
            e.iseq = iseq; e.seq = seq; e.tPub = tPub; e.arg = 0; e.symbol = sym; e.ctl = Control.None; e.ev = ev;
        }

        public void onEvent(CmdMsg m, long seq, boolean eob) {
            if (m.ctl == Control.None) {
                if (journal != null) journal.pushCmd(fmt, m.iseq, m.symbol, m.cmd);  // journal-before-apply
                iseq = m.iseq;
                tPub = m.tPub;
                nCommands++;
                core.apply(m.symbol, m.cmd, this);
            } else {
                // the new segment starts at this cut, before the snapshot is reported
                if (m.ctl == Control.Checkpoint && journal != null) seg.rotate(journal, m.iseq);
                if (m.ctl == Control.Snapshot || m.ctl == Control.Checkpoint) {
                    List<Block> blocks = new ArrayList<>();
                    core.snapshotBlocks(blocks);
                    synchronized (sh.snaps) {
                        SnapState st = sh.snaps.get(m.arg);
                        if (st != null) {
                            st.blocks.addAll(blocks);
                            st.cut = m.iseq;
                            st.remaining--;
                        }
                        sh.snaps.notifyAll();
                    }
                }
                if (m.ctl == Control.Shutdown) {
                    stop = true;
                    if (journal != null) {
                        String e = journal.finish();
                        if (e != null) sh.fail("journal: " + e);
                    }
                } else {
                    force = true;
                }
                EvtMsg e = out.stage();
                if (e != null) { e.iseq = m.iseq; e.seq = 0; e.tPub = 0; e.arg = m.arg; e.symbol = 0; e.ctl = m.ctl; e.ev = null; }
            }
            if (eob) out.commit();
        }

        void run() {
            long lastHandoff = System.nanoTime();
            for (;;) {
                force = false;
                int n = inbox.poll(this);
                if (n > 0) { counters.commands = nCommands; counters.events = nEvents; }
                if (stop || inbox.isAlerted()) break;
                if (journal != null && journal.pending() > 0
                        && (force || (n == 0 && System.nanoTime() - lastHandoff >= 50_000))) {
                    journal.handOff();
                    lastHandoff = System.nanoTime();
                }
                if (n == 0) inbox.idle(); else inbox.resetIdle();
            }
            out.commit();
        }
    }

    private static void egressLoop(Shared sh, List<EgressPart> parts, boolean journaled) {
        int idleOn = 0;
        final boolean[] stop = {false};
        for (;;) {
            int total = 0, live = 0;
            for (int i = 0; i < parts.size(); i++) {
                EgressPart ep = parts.get(i);
                if (ep.stopped) continue;
                live++;
                idleOn = i;
                stop[0] = false;
                int n = ep.outbox.poll((m, seq, eob) -> {
                    if (m.ctl == Control.None) {
                        ep.lastIseq = m.iseq;
                        for (Egress pl : ep.plugs) pl.onEvent(m);
                    } else if (m.ctl == Control.Barrier) {
                        for (Egress pl : ep.plugs) { pl.onBatchEnd(); pl.onIdle(); }
                        sh.egressEpoch[ep.p].set(m.arg);
                    } else if (m.ctl == Control.Shutdown) {
                        stop[0] = true;
                    } else if (m.ctl == Control.Checkpoint) {
                        for (Egress pl : ep.plugs) pl.onCheckpoint(m.iseq);
                    }
                    if (eob) for (Egress pl : ep.plugs) pl.onBatchEnd();
                });
                total += n;
                if (stop[0]) {
                    if (journaled) {
                        AtomicLong d = ep.ctx.durable();
                        long last = ep.lastIseq;
                        waitUntil(sh, () -> Long.compareUnsigned(d.get(), last) >= 0);
                    }
                    for (Egress pl : ep.plugs) { pl.onIdle(); pl.onShutdown(); }
                    ep.stopped = true;
                } else if (n == 0) {
                    for (Egress pl : ep.plugs) pl.onIdle();
                }
            }
            if (live == 0) return;
            boolean alerted = false;
            for (EgressPart ep : parts) alerted |= ep.outbox.isAlerted();
            if (alerted) return;
            if (total == 0) parts.get(idleOn).outbox.idle(); else parts.get(idleOn).outbox.resetIdle();
        }
    }
}
