//! Operational statistics (not part of the spec contract): ring depths,
//! per-partition counters, journal watermarks and fsync timings, and a
//! Prometheus text-format rendering.
package io.github.abhijitkrm.orderer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

public final class Stats {
    private Stats() {}

    /// fsync timings one journal I/O thread records.
    public static final class IoStats {
        final AtomicLong fsyncs = new AtomicLong(), fsyncNsTotal = new AtomicLong(), fsyncNsMax = new AtomicLong();
        void record(long ns) {
            fsyncs.incrementAndGet();
            fsyncNsTotal.addAndGet(ns);
            fsyncNsMax.accumulateAndGet(ns, Math::max);
        }
    }

    /// Counters an engine thread publishes once per batch.
    static final class EngineCounters {
        volatile long commands, events;
    }

    /// One partition's view. flushed/durable are -1 (unsigned max) without journals.
    public record PartitionStats(int partition, long inboxDepth, long outboxDepth, long commands, long events,
                                 long flushedIseq, long durableIseq, long fsyncs, long fsyncNsTotal, long fsyncNsMax) {}

    /// A point-in-time view (fields individually exact, not mutually consistent).
    public record PipelineStats(long ingressDepth, List<PartitionStats> partitions) {
        /// Prometheus text exposition format (version 0.0.4).
        public String toPrometheus() {
            StringBuilder out = new StringBuilder();
            out.append("# HELP orderer_ingress_depth Commands published, not yet routed.\n# TYPE orderer_ingress_depth gauge\n")
               .append("orderer_ingress_depth ").append(ingressDepth).append('\n');
            series(out, "inbox_depth", "Commands routed, not yet applied.", "gauge", PartitionStats::inboxDepth);
            series(out, "outbox_depth", "Events staged, not yet consumed by egress.", "gauge", PartitionStats::outboxDepth);
            series(out, "commands_total", "Commands applied.", "counter", PartitionStats::commands);
            series(out, "events_total", "Events emitted.", "counter", PartitionStats::events);
            series(out, "durable_iseq", "Highest iseq covered by a completed fsync.", "gauge", PartitionStats::durableIseq);
            series(out, "fsyncs_total", "Journal fsyncs.", "counter", PartitionStats::fsyncs);
            series(out, "fsync_ns_total", "Time spent in journal fsync, in nanoseconds.", "counter", PartitionStats::fsyncNsTotal);
            series(out, "fsync_max_ns", "Longest journal fsync, in nanoseconds.", "gauge", PartitionStats::fsyncNsMax);
            return out.toString();
        }

        private void series(StringBuilder out, String name, String help, String kind, ToLongFunction<PartitionStats> get) {
            out.append("# HELP orderer_").append(name).append(' ').append(help).append("\n# TYPE orderer_").append(name)
               .append(' ').append(kind).append('\n');
            for (PartitionStats p : partitions)
                out.append("orderer_").append(name).append("{partition=\"").append(p.partition()).append("\"} ")
                   .append(Long.toUnsignedString(get.applyAsLong(p))).append('\n');
        }
    }
}
