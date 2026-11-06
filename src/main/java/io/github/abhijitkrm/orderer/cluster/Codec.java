package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.OType;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.Consensus.Disk;
import io.github.abhijitkrm.orderer.Consensus.Entry;
import io.github.abhijitkrm.orderer.Consensus.Msg;
import io.github.abhijitkrm.orderer.Consensus.MsgKind;
import io.github.abhijitkrm.orderer.Consensus.Persist;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/// Binary encodings shared with orderer-rust, orderer-cpp and orderer-go:
/// protocol messages, WAL frame bodies and command-batch log entry payloads.
/// Little-endian. Decoders never read out of bounds; they throw CodecException
/// on malformed input.
public final class Codec {
    private Codec() {}

    public static final int MAX_ENTRY = 16 << 20;
    public static final int CMD_BYTES = 32;
    public static final int TAG_BATCH = 1, TAG_SESSION_BATCH = 2;

    public static final class CodecException extends RuntimeException {
        public CodecException(String m) { super("codec: " + m); }
    }

    /// A bounds-checked cursor.
    public static final class Reader {
        final byte[] b;
        int pos;
        final int end;

        public Reader(byte[] b) { this(b, 0, b.length); }
        public Reader(byte[] b, int off, int n) { this.b = b; this.pos = off; this.end = off + n; }

        public int remaining() { return end - pos; }

        int take(long n) {
            if (n < 0 || n > end - pos) throw new CodecException("truncated input");
            int at = pos;
            pos += (int) n;
            return at;
        }

        public int u8() { return b[take(1)] & 0xFF; }
        public long u32() { return Buf.u32(b, take(4)); }
        public long u64() { return Buf.u64(b, take(8)); }
        public boolean flag() {
            int v = u8();
            if (v > 1) throw new CodecException("flag");
            return v == 1;
        }
        public byte[] bytes(long n, boolean copy) {
            int at = take(n);
            return Arrays.copyOfRange(b, at, at + (int) n);
        }
        public void finish() { if (pos != end) throw new CodecException("trailing bytes"); }
    }

    static void putEntries(Buf o, List<Entry> es) {
        o.u32(es.size());
        for (Entry e : es) {
            o.u32(e.term());
            o.u32(e.data().length);
            o.bytes(e.data());
        }
    }

    static List<Entry> getEntries(Reader r) {
        long n = r.u32();
        // Each entry is at least 8 bytes; reject counts the input cannot hold.
        if (n > r.remaining() / 8) throw new CodecException("entry count");
        List<Entry> es = new ArrayList<>((int) n);
        for (long i = 0; i < n; i++) {
            long term = r.u32();
            long len = r.u32();
            if (len > MAX_ENTRY) throw new CodecException("entry too large");
            es.add(new Entry(term, r.bytes(len, true)));
        }
        return es;
    }

    /// An upper bound on encodeMsg's output, to size buffers once.
    public static int encodedSize(Msg m) {
        int n = 32;
        if (m.kind == MsgKind.APPEND) for (Entry e : m.entries) n += 8 + e.data().length;
        return n;
    }

    public static Buf encodeMsg(Msg m, Buf o) {
        switch (m.kind) {
            case VOTE -> o.u8(1).u32(m.term).u64(m.lastIndex).u32(m.lastTerm).bool(m.pre);
            case VOTE_REPLY -> o.u8(2).u32(m.term).bool(m.granted).bool(m.pre);
            case APPEND -> { o.u8(3).u32(m.term).u64(m.prevIndex).u32(m.prevTerm).u64(m.commit); putEntries(o, m.entries); }
            case APPEND_REPLY -> o.u8(4).u32(m.term).bool(m.success).u64(m.index);
        }
        return o;
    }

    public static Msg decodeMsg(Reader r) {
        switch (r.u8()) {
            case 1: { long t = r.u32(), li = r.u64(), lt = r.u32(); return Msg.vote(t, li, lt, r.flag()); }
            case 2: { long t = r.u32(); boolean g = r.flag(); return Msg.voteReply(t, g, r.flag()); }
            case 3: {
                long t = r.u32(), pi = r.u64(), pt = r.u32(), c = r.u64();
                return Msg.append(t, pi, pt, getEntries(r), c);
            }
            case 4: { long t = r.u32(); boolean s = r.flag(); return Msg.appendReply(t, s, r.u64()); }
            default: throw new CodecException("message tag");
        }
    }

    public static Msg decodeMsg(byte[] b) {
        Reader r = new Reader(b);
        Msg m = decodeMsg(r);
        r.finish();
        return m;
    }

    /// A WAL frame body: one atomic consensus write (the token is not stored).
    public static Buf encodePersist(Persist p, Buf o) {
        o.u32(p.term);
        o.u8(p.votedFor < 0 ? 255 : p.votedFor);
        if (p.hasCompact) o.u8(1).u64(p.compactIndex).u32(p.compactTerm);
        else o.u8(0);
        o.u64(p.from);
        putEntries(o, p.entries);
        return o;
    }

    public static Persist decodePersist(byte[] d, int off, int n) {
        Reader r = new Reader(d, off, n);
        Persist p = new Persist();
        p.term = r.u32();
        int v = r.u8();
        p.votedFor = v == 255 ? -1 : v;
        switch (r.u8()) {
            case 0 -> {}
            case 1 -> { p.hasCompact = true; p.compactIndex = r.u64(); p.compactTerm = r.u32(); }
            default -> throw new CodecException("compact flag");
        }
        p.from = r.u64();
        if (p.from == 0) throw new CodecException("write starts at index 0");
        p.entries = getEntries(r);
        r.finish();
        return p;
    }

    /// Apply one write to a recovered disk image (the protocol core's Disk
    /// rules). Writes must be applied in issue order.
    public static void applyPersist(Disk d, Persist p) {
        if (!(d.log instanceof ArrayList)) d.log = new ArrayList<>(d.log);
        d.term = p.term;
        d.votedFor = p.votedFor;
        if (p.hasCompact && p.compactIndex > d.base) {
            long drop = Math.min(p.compactIndex - d.base, d.log.size());
            d.log.subList(0, (int) drop).clear();
            d.base = p.compactIndex;
            d.baseTerm = p.compactTerm;
        }
        long keep = p.from - 1 - d.base;
        if (keep < d.log.size()) d.log.subList((int) keep, d.log.size()).clear();
        d.log.addAll(p.entries);
    }

    // ---- command batches: the payload of a log entry ----

    public record SessionKey(long client, long seq) {}

    /// Decoded commands, parallel arrays ready for Pipeline.Handle#publishBatch.
    public record Batch(long[] syms, Command[] cmds) {
        public int size() { return cmds.length; }
    }

    /// One command as the journal record's bytes 8..40 (symbol, kind, side,
    /// otype, tif, order id, price, qty) without iseq, checksum or padding.
    static void putCmd32(byte[] r, int at, long sym, Command c) {
        // The caller reserved these bytes with Buf.skip, which zeroes them.
        Buf.put32(r, at, sym);
        if (c instanceof Command.New n) {
            r[at + 4] = 1;
            r[at + 5] = (byte) n.side().ordinal();
            r[at + 6] = (byte) n.otype().ordinal();
            r[at + 7] = (byte) n.tif().ordinal();
            Buf.put64(r, at + 8, n.orderId());
            Buf.put64(r, at + 16, n.price());
            Buf.put64(r, at + 24, n.qty());
        } else if (c instanceof Command.Cancel x) {
            r[at + 4] = 2;
            Buf.put64(r, at + 8, x.orderId());
        } else {
            Command.Replace x = (Command.Replace) c;
            r[at + 4] = 3;
            Buf.put64(r, at + 8, x.orderId());
            Buf.put64(r, at + 16, x.price());
            Buf.put64(r, at + 24, x.qty());
        }
    }

    /// n commands: 0x01 | n u32 | n*32 bytes; the session form is
    /// 0x02 | client u64 | seq u64 | n | ...
    public static Buf encodeBatch(long[] syms, Command[] cmds, int n, Buf o, SessionKey session) {
        if (session != null) o.u8(TAG_SESSION_BATCH).u64(session.client()).u64(session.seq());
        else o.u8(TAG_BATCH);
        o.u32(n);
        int at = o.skip(n * CMD_BYTES);
        for (int i = 0; i < n; i++) putCmd32(o.data, at + i * CMD_BYTES, syms[i], cmds[i]);
        return o;
    }

    public static byte[] encodeBatch(long[] syms, Command[] cmds, SessionKey session) {
        return encodeBatch(syms, cmds, cmds.length, new Buf(32 * cmds.length + 24), session).toArray();
    }

    /// The session a payload belongs to, or null, without decoding commands.
    public static SessionKey sessionOf(byte[] d) {
        if (d.length < 17 || d[0] != TAG_SESSION_BATCH) return null;
        return new SessionKey(Buf.u64(d, 1), Buf.u64(d, 9));
    }

    /// Decode a plain or session-tagged payload. Empty is a leader's no-op.
    public static Batch decodeBatch(byte[] d) {
        if (d.length == 0) return new Batch(new long[0], new Command[0]);
        Reader r = new Reader(d);
        switch (r.u8()) {
            case TAG_BATCH -> {}
            case TAG_SESSION_BATCH -> { r.u64(); r.u64(); }
            default -> throw new CodecException("entry tag");
        }
        long count = r.u32();
        if (count != r.remaining() / CMD_BYTES || r.remaining() % CMD_BYTES != 0) throw new CodecException("batch length");
        long[] syms = new long[(int) count];
        Command[] cmds = new Command[(int) count];
        for (int i = 0; i < count; i++) {
            int at = r.take(CMD_BYTES);
            syms[i] = Buf.u32(d, at);
            long id = Buf.u64(d, at + 8), price = Buf.u64(d, at + 16), qty = Buf.u64(d, at + 24);
            switch (d[at + 4]) {
                case 1 -> {
                    if ((d[at + 5] & 0xFF) > 1 || (d[at + 6] & 0xFF) > 1 || (d[at + 7] & 0xFF) > 3) throw new CodecException("command");
                    cmds[i] = new Command.New(id, Side.values()[d[at + 5]], OType.values()[d[at + 6]], price, qty, Tif.values()[d[at + 7]]);
                }
                case 2 -> cmds[i] = new Command.Cancel(id);
                case 3 -> cmds[i] = new Command.Replace(id, price, qty);
                default -> throw new CodecException("command");
            }
        }
        return new Batch(syms, cmds);
    }
}
