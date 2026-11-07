package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.Consensus.Entry;
import java.util.Map;
import java.util.TreeMap;

/// Applies a session's batch only when seq is newer than the last applied for
/// that client, so a retry that landed in the log twice takes effect once. The
/// table is part of every snapshot and digest.
public final class DedupSm implements StateMachine {
    private final StateMachine inner;
    private Map<Long, Long> last = new TreeMap<>(Long::compareUnsigned);
    private long duplicates;

    public DedupSm(StateMachine inner) { this.inner = inner; }

    @Override public void apply(long index, Entry e) {
        Codec.SessionKey s = Codec.sessionOf(e.data());
        if (s != null) {
            long prev = last.getOrDefault(s.client(), 0L);
            if (Long.compareUnsigned(s.seq(), prev) <= 0) { duplicates++; return; }
            last.put(s.client(), s.seq());
        }
        inner.apply(index, e);
    }

    private Buf table() {
        Buf o = new Buf().u32(last.size());
        for (Map.Entry<Long, Long> e : last.entrySet()) o.u64(e.getKey()).u64(e.getValue());
        return o;
    }

    @Override public byte[] snapshot(long index) {
        Buf o = table();
        byte[] in = inner.snapshot(index);
        o.u64(in.length).bytes(in);
        return o.toArray();
    }

    @Override public void restore(long index, byte[] d) {
        if (d.length < 4) throw new RestoreException("short dedup snapshot");
        long count = Buf.u32(d, 0);
        long tableEnd = 4 + count * 16;
        if (d.length < tableEnd + 8) throw new RestoreException("short dedup snapshot");
        Map<Long, Long> m = new TreeMap<>(Long::compareUnsigned);
        for (long i = 0; i < count; i++) m.put(Buf.u64(d, (int) (4 + i * 16)), Buf.u64(d, (int) (12 + i * 16)));
        long n = Buf.u64(d, (int) tableEnd);
        if (n < 0 || n > d.length - tableEnd - 8) throw new RestoreException("short dedup snapshot");
        inner.restore(index, java.util.Arrays.copyOfRange(d, (int) tableEnd + 8, (int) (tableEnd + 8 + n)));
        last = m;
    }

    @Override public byte[] digest(long index) {
        byte[] in = inner.digest(index);
        if (in == null) return null;
        Buf b = table().bytes(in);
        return RecorderSm.sha256(b.toArray());
    }

    @Override public byte[] deepDigest(long index) {
        byte[] in = inner.deepDigest(index);
        if (in == null) return null;
        return RecorderSm.sha256(table().bytes(in).toArray());
    }

    public long duplicates() { return duplicates; }
}
