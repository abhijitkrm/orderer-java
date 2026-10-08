//! Ring slot types and pluggable per-partition event consumers
//! (spec/PIPELINE.md §7).
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Event;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/// One partition's consumer of events.
public interface Egress {
    void onEvent(EvtMsg m);           // every event, partition order
    default void onBatchEnd() {}      // after a ring batch / before a drain completes
    default void onIdle() {}          // while idle: release gated work
    default void onShutdown() {}      // once, after every event

    /// Control operations ride the rings so they cut every partition at the
    /// same point of the ingress order (spec/PIPELINE.md §6).
    enum Control { None, Barrier, Snapshot, Shutdown }

    /// Ingress / inbox slot (mutable flyweight).
    final class CmdMsg {
        public long iseq, tPub, arg, symbol;
        public Control ctl = Control.None;
        public Command cmd;
        void copyFrom(CmdMsg m) { iseq = m.iseq; tPub = m.tPub; arg = m.arg; symbol = m.symbol; ctl = m.ctl; cmd = m.cmd; }
    }

    /// Outbox slot: one event (or a control passing through to egress).
    final class EvtMsg {
        public long iseq, seq, tPub, arg, symbol;
        public Control ctl = Control.None;
        public Event ev;
        public EvtMsg copy() {
            EvtMsg c = new EvtMsg();
            c.iseq = iseq; c.seq = seq; c.tPub = tPub; c.arg = arg; c.symbol = symbol; c.ctl = ctl; c.ev = ev;
            return c;
        }
    }

    /// What a partition's egress instances know.
    record Ctx(int partition, int partitions, long epochNanos, AtomicLong durable) {
        /// Highest iseq covered by a completed fsync (-1 = unsigned max without journals).
        public long durableIseq() { return durable.get(); }
        public long nowNanos() { return System.nanoTime() - epochNanos; }
    }

    @FunctionalInterface
    interface Factory { Egress create(Ctx ctx); }

    @FunctionalInterface
    interface Fn { void accept(int partition, EvtMsg m); }

    // ---- Collect: canonical lines per partition (harnesses, tests) -----------------------------

    final class CollectHandle {
        private final List<StringBuilder> bufs = new ArrayList<>();
        synchronized void ensure(int n) { while (bufs.size() < n) bufs.add(new StringBuilder()); }
        synchronized void append(int p, CharSequence s) { bufs.get(p).append(s); }
        public synchronized List<String> take() {
            List<String> out = new ArrayList<>();
            for (StringBuilder b : bufs) { out.add(b.toString()); b.setLength(0); }
            return out;
        }
        /// spec/HARNESS.md §3 listing: partition 0's lines, then 1's, …
        public synchronized String listing() {
            StringBuilder sb = new StringBuilder();
            for (StringBuilder b : bufs) sb.append(b);
            return sb.toString();
        }
    }

    record Collect(Factory factory, CollectHandle handle) {}

    static Collect collect(boolean tagged) {
        CollectHandle h = new CollectHandle();
        Factory f = ctx -> {
            h.ensure(ctx.partitions());
            StringBuilder local = new StringBuilder(1 << 16);
            return new Egress() {
                public void onEvent(EvtMsg m) {
                    local.append(tagged ? Event.canonical(m.seq, m.symbol, m.ev) : Event.canonical(m.seq, m.ev)).append('\n');
                }
                public void onBatchEnd() {
                    if (local.length() == 0) return;
                    h.append(ctx.partition(), local);
                    local.setLength(0);
                }
                public void onShutdown() { onBatchEnd(); }
            };
        };
        return new Collect(f, h);
    }

    // ---- Callback ------------------------------------------------------------------------------

    /// f(partition, msg) for every event. The message is the ring slot: copy it to keep it.
    static Factory callback(Fn f) {
        return ctx -> m -> f.accept(ctx.partition(), m);
    }

    // ---- Acks: durability-gated delivery --------------------------------------------------------

    /// f(partition, msg) only once the causing command is durable (spec/PIPELINE.md §5).
    static Factory acks(Fn f) {
        return ctx -> new Egress() {
            final ArrayDeque<EvtMsg> pending = new ArrayDeque<>();
            void release() {
                if (pending.isEmpty()) return;
                long d = ctx.durableIseq();
                while (!pending.isEmpty() && Long.compareUnsigned(pending.peekFirst().iseq, d) <= 0)
                    f.accept(ctx.partition(), pending.pollFirst());
            }
            public void onEvent(EvtMsg m) { pending.addLast(m.copy()); }
            public void onBatchEnd() { release(); }
            public void onIdle() { release(); }
            public void onShutdown() { release(); }
        };
    }

    // ---- Metrics: counts + end-to-end latency ---------------------------------------------------

    final class PartitionMetrics {
        public int partition;
        public long events, commands, trades;
        public long[] latencies;  // ns, arrival order
        public int samples;
    }

    final class MetricsHandle {
        private final List<PartitionMetrics> out = new ArrayList<>();
        synchronized void add(PartitionMetrics m) { out.add(m); }
        public synchronized List<PartitionMetrics> results() { return new ArrayList<>(out); }
    }

    record Metrics(Factory factory, MetricsHandle handle) {}

    /// One latency sample per command, at its first event: now - tPub
    /// (spec/BENCH.md §2.2 step 5). Samples preallocated per partition.
    static Metrics metrics(int sampleCapacity) {
        MetricsHandle h = new MetricsHandle();
        Factory f = ctx -> {
            PartitionMetrics m = new PartitionMetrics();
            m.partition = ctx.partition();
            m.latencies = new long[sampleCapacity];
            return new Egress() {
                long lastIseq;
                public void onEvent(EvtMsg e) {
                    m.events++;
                    if (e.ev instanceof Event.Trade) m.trades++;
                    if (e.iseq != lastIseq) {
                        lastIseq = e.iseq;
                        m.commands++;
                        if (e.tPub != 0 && m.samples < m.latencies.length) {
                            long now = ctx.nowNanos();
                            m.latencies[m.samples++] = Math.max(now - e.tPub, 0);
                        }
                    }
                }
                public void onShutdown() { h.add(m); }
            };
        };
        return new Metrics(f, h);
    }
}
