//! A rolling hash of each partition's event stream (spec/REPLICATION.md 13.16).
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.Types.CloseReason;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.matcher.Types.RejectReason;
import java.util.concurrent.atomic.AtomicLongArray;

/// Keeps one 64-bit rolling hash per partition over the events it emits.
/// Replicas fed the same commands emit the same events, so their chains agree,
/// and a divergence in behaviour shows up as a mismatch. Reading is O(partitions):
/// no snapshot of the books. It does not see state that has not yet influenced an
/// event; compare full snapshots (the deep digest) for that.
///
/// Register it as an egress (`builder.egress(digest.factory())`); read values
/// after a pipeline drain.
public final class EventDigest {
    private final AtomicLongArray slots;

    /// A digest for {@code partitions} partitions, each chain starting at the given
    /// value (zeros for a fresh pipeline, the snapshot's values after a restore).
    public EventDigest(int partitions, long[] initial) {
        slots = new AtomicLongArray(partitions);
        for (int i = 0; i < partitions && initial != null && i < initial.length; i++) slots.set(i, initial[i]);
    }

    /// One step of the chain: rotl((h ^ x) * K, 31). Pinned exactly: replicas of different languages compare these.
    public static long step(long h, long x) { return Long.rotateLeft((h ^ x) * 0x9E3779B97F4A7C15L, 31); }

    /// Fold one event into a chain: causing iseq, symbol and kind, per-book seq, then the kind's fields.
    /// Reasons are coded explicitly, not by enum order; signed prices as their two's complement.
    public static long chainEvent(long h, Egress.EvtMsg m) {
        Event ev = m.ev;
        long kind = ev instanceof Event.Accepted ? 1 : ev instanceof Event.Rejected ? 2 : ev instanceof Event.Trade ? 3
                : ev instanceof Event.Closed ? 4 : 5;
        h = step(h, m.iseq);
        h = step(h, (m.symbol << 8) | kind);
        h = step(h, m.seq);
        if (ev instanceof Event.Accepted e) {
            h = step(h, e.orderId());
            return step(h, e.leavesQty());
        } else if (ev instanceof Event.Rejected e) {
            h = step(h, e.orderId());
            return step(h, rejectCode(e.reason()));
        } else if (ev instanceof Event.Trade e) {
            h = step(h, e.maker());
            h = step(h, e.taker());
            h = step(h, e.price());
            return step(h, e.qty());
        } else if (ev instanceof Event.Closed e) {
            h = step(h, e.orderId());
            return step(h, closeCode(e.reason()));
        }
        Event.Replaced e = (Event.Replaced) ev;
        h = step(h, e.orderId());
        h = step(h, e.price());
        return step(h, e.qty());
    }

    static long rejectCode(RejectReason r) {
        return switch (r) {
            case InvalidQty -> 0; case InvalidPrice -> 1; case DuplicateOrderId -> 2; case UnknownOrderId -> 3;
            case PostOnlyWouldCross -> 4; case FokCannotFill -> 5; case BookFull -> 6;
        };
    }

    static long closeCode(CloseReason r) {
        return switch (r) { case Filled -> 0; case Cancelled -> 1; case Expired -> 2; };
    }

    /// The egress factory to register with the pipeline builder.
    public Egress.Factory factory() {
        return ctx -> new Egress() {
            long h = slots.get(ctx.partition());
            public void onEvent(Egress.EvtMsg m) { h = chainEvent(h, m); }
            // Published at batch ends and before a drain completes, not per event.
            public void onBatchEnd() { slots.set(ctx.partition(), h); }
            public void onShutdown() { slots.set(ctx.partition(), h); }
        };
    }

    /// Every partition's chain, in partition order (current as of the last drain).
    public long[] values() {
        long[] v = new long[slots.length()];
        for (int i = 0; i < v.length; i++) v[i] = slots.get(i);
        return v;
    }
}
