package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.Consensus.Entry;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/// Records every non-empty entry. Snapshot is the list `index u64 | len u32 |
/// data`; the digest is SHA-256 of that list. Identical to orderer-rust's RecorderSm.
public final class RecorderSm implements StateMachine {
    public record Recorded(long index, byte[] data) {}

    private List<Recorded> applied = new ArrayList<>();

    @Override public void apply(long index, Entry e) {
        if (e.data().length > 0) applied.add(new Recorded(index, e.data()));
    }

    private byte[] serialize() {
        Buf o = new Buf();
        for (Recorded r : applied) o.u64(r.index()).u32(r.data().length).bytes(r.data());
        return o.toArray();
    }

    @Override public byte[] snapshot(long index) { return serialize(); }

    @Override public void restore(long index, byte[] d) {
        List<Recorded> v = new ArrayList<>();
        int at = 0;
        while (at < d.length) {
            if (d.length - at < 12) throw new RestoreException("short recorder snapshot");
            long i = Buf.u64(d, at), len = Buf.u32(d, at + 8);
            if (at + 12 + len > d.length) throw new RestoreException("short recorder snapshot");
            v.add(new Recorded(i, java.util.Arrays.copyOfRange(d, at + 12, at + 12 + (int) len)));
            at += 12 + (int) len;
        }
        applied = v;
    }

    @Override public byte[] digest(long index) { return sha256(serialize()); }

    public List<Recorded> applied() { return applied; }

    static byte[] sha256(byte[] b) {
        try { return MessageDigest.getInstance("SHA-256").digest(b); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
