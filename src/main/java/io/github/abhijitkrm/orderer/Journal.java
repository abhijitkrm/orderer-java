//! Per-partition journals (spec/JOURNAL.md): naming, JSONL and binary
//! encodings, strict readers, and the asynchronous chunk writer.
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.PriceIndex;
import io.github.abhijitkrm.matcher.Types.CloseReason;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.matcher.Types.OType;
import io.github.abhijitkrm.matcher.Types.RejectReason;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Journal {
    private Journal() {}

    public enum Format { Jsonl, Binary }
    public enum Kind {
        Cmd(1, 40), Evt(2, 48);
        final int code, recordSize;
        Kind(int code, int size) { this.code = code; this.recordSize = size; }
        String label() { return this == Cmd ? "cmd" : "evt"; }
    }

    public static final int HEADER = 64, CMD_RECORD = 40, EVT_RECORD = 48, MAX_RECORD = 256;

    /// When the I/O thread fsyncs. Never observable (spec/PIPELINE.md §8).
    public record FsyncPolicy(Mode mode, long n, long idleNanos, long intervalNanos) {
        public enum Mode { Never, EveryN, Every }
        public static FsyncPolicy never() { return new FsyncPolicy(Mode.Never, 0, 0, 0); }
        /// Group commit once n records are pending, or after idle with no more.
        public static FsyncPolicy everyN(long n) { return new FsyncPolicy(Mode.EveryN, n, 200_000, 0); }
        public static FsyncPolicy every(long nanos) { return new FsyncPolicy(Mode.Every, 0, nanos, nanos); }
        public FsyncPolicy withIdle(long nanos) { return new FsyncPolicy(mode, n, nanos, intervalNanos); }
    }

    public record Config2(Path dir, Format format, FsyncPolicy fsync, boolean events, boolean append) {
        public static Config2 of(Path dir, Format f) { return new Config2(dir, f, FsyncPolicy.everyN(1024), true, false); }
        public Config2 withFsync(FsyncPolicy p) { return new Config2(dir, format, p, events, append); }
        public Config2 withEvents(boolean e) { return new Config2(dir, format, fsync, e, append); }
        public Config2 withAppend(boolean a) { return new Config2(dir, format, fsync, events, a); }
    }

    public static final class CorruptJournal extends RuntimeException {
        public CorruptJournal(String m) { super(m); }
    }

    public static Path path(Path dir, Kind k, int p, Format f) {
        return dir.resolve(k.label() + "-" + p + (f == Format.Jsonl ? ".journal" : ".bin"));
    }

    // ---- encodings ---------------------------------------------------------------------------

    public static String jsonlHeader(Kind k, int p, int P, Config b) {
        return "{\"format\":\"orderer-journal/1\",\"kind\":\"" + k.label() + "\",\"partition\":" + p
                + ",\"partitions\":" + P + ",\"pmin\":" + b.priceMin() + ",\"pmax\":" + b.priceMax()
                + ",\"max_orders\":" + b.maxOrders() + ",\"index\":\"" + Flat.indexName(b.index()) + "\"}\n";
    }

    public static byte[] binaryHeader(Kind k, int p, int P, Config b) {
        ByteBuffer h = ByteBuffer.allocate(HEADER).order(ByteOrder.LITTLE_ENDIAN);
        h.put("ORDJ".getBytes(StandardCharsets.US_ASCII));
        h.putShort((short) 1);
        h.put((byte) k.code);
        h.put((byte) (b.index() == PriceIndex.Kind.Tree ? 1 : 0));
        h.putInt(p).putInt(P).putInt(k.recordSize).putInt(0);
        h.putLong(b.priceMin()).putLong(b.priceMax()).putLong(b.maxOrders());
        return h.array();
    }

    /// JSONL command record line (no newline): canonical engine line + "iseq".
    public static void writeCmdLine(long iseq, long sym, Command c, StringBuilder out) {
        Flat.writeCommand(c, sym, out);
        out.setLength(out.length() - 1);
        out.append(",\"iseq\":").append(Long.toUnsignedString(iseq)).append('}');
    }

    public static void encodeCmd(long iseq, long sym, Command c, ByteBuffer r) {
        int base = r.position();
        r.putLong(iseq).putInt((int) sym);
        if (c instanceof Command.New n) {
            r.put((byte) 1).put((byte) (n.side() == Side.Ask ? 1 : 0)).put((byte) (n.otype() == OType.Market ? 1 : 0))
             .put((byte) n.tif().ordinal()).putLong(n.orderId()).putLong(n.price()).putLong(n.qty());
        } else if (c instanceof Command.Cancel x) {
            r.put((byte) 2).put((byte) 0).put((byte) 0).put((byte) 0).putLong(x.orderId()).putLong(0).putLong(0);
        } else {
            Command.Replace x = (Command.Replace) c;
            r.put((byte) 3).put((byte) 0).put((byte) 0).put((byte) 0).putLong(x.orderId()).putLong(x.price()).putLong(x.qty());
        }
        assert r.position() - base == CMD_RECORD;
    }

    public record CmdRecord(long iseq, long sym, Command cmd) {}

    public static CmdRecord decodeCmd(ByteBuffer r) {
        long iseq = r.getLong(), sym = r.getInt() & 0xFFFFFFFFL;
        int kind = r.get(), side = r.get(), otype = r.get(), tif = r.get();
        long id = r.getLong(), price = r.getLong(), qty = r.getLong();
        switch (kind) {
            case 1:
                if (side > 1 || side < 0 || otype > 1 || otype < 0 || tif < 0 || tif > 3) return null;
                return new CmdRecord(iseq, sym, new Command.New(id, side == 1 ? Side.Ask : Side.Bid,
                        otype == 1 ? OType.Market : OType.Limit, price, qty, Tif.values()[tif]));
            case 2: return new CmdRecord(iseq, sym, new Command.Cancel(id));
            case 3: return new CmdRecord(iseq, sym, new Command.Replace(id, price, qty));
            default: return null;
        }
    }

    /// spec/JOURNAL.md §3.2: reject 1..7 in SPEC order, close 1..3.
    public static void encodeEvt(long seq, long sym, Event e, ByteBuffer r) {
        r.putLong(seq).putInt((int) sym);
        long a = 0, b = 0, c = 0, d = 0;
        int ev, reason = 0;
        if (e instanceof Event.Accepted x) { ev = 1; a = x.orderId(); d = x.leavesQty(); }
        else if (e instanceof Event.Rejected x) { ev = 2; reason = x.reason().ordinal() + 1; a = x.orderId(); }
        else if (e instanceof Event.Trade x) { ev = 3; a = x.maker(); b = x.taker(); c = x.price(); d = x.qty(); }
        else if (e instanceof Event.Closed x) { ev = 4; reason = x.reason().ordinal() + 1; a = x.orderId(); }
        else { Event.Replaced x = (Event.Replaced) e; ev = 5; a = x.orderId(); c = x.price(); d = x.qty(); }
        r.put((byte) ev).put((byte) reason).putShort((short) 0).putLong(a).putLong(b).putLong(c).putLong(d);
    }

    public record EvtRecord(long seq, long sym, Event ev) {}

    public static EvtRecord decodeEvt(ByteBuffer r) {
        long seq = r.getLong(), sym = r.getInt() & 0xFFFFFFFFL;
        int ev = r.get(), reason = r.get();
        r.getShort();
        long a = r.getLong(), b = r.getLong(), c = r.getLong(), d = r.getLong();
        Event e;
        switch (ev) {
            case 1: if (reason != 0) return null; e = new Event.Accepted(a, d); break;
            case 2: if (reason < 1 || reason > 7) return null; e = new Event.Rejected(a, RejectReason.values()[reason - 1]); break;
            case 3: if (reason != 0) return null; e = new Event.Trade(a, b, c, d); break;
            case 4: if (reason < 1 || reason > 3) return null; e = new Event.Closed(a, CloseReason.values()[reason - 1]); break;
            case 5: if (reason != 0) return null; e = new Event.Replaced(a, c, d); break;
            default: return null;
        }
        return new EvtRecord(seq, sym, e);
    }

    // ---- reading (recovery) ------------------------------------------------------------------

    public record Header(Kind kind, int partition, int partitions, Config book) {
        boolean same(Header o) {
            return kind == o.kind && partition == o.partition && partitions == o.partitions && Flat.sameBook(book, o.book);
        }
    }

    static CorruptJournal corrupt(Path p, String d) { return new CorruptJournal(p + ": " + d); }

    static byte[] readAll(Path p) {
        try { return Files.readAllBytes(p); }
        catch (IOException e) { throw corrupt(p, e.getMessage()); }
    }

    static Header parseJsonlHeader(Path p, String line) {
        if (!"orderer-journal/1".equals(Flat.get(line, "format"))) throw corrupt(p, "not an orderer-journal/1 header");
        String k = Flat.get(line, "kind");
        Kind kind = "cmd".equals(k) ? Kind.Cmd : "evt".equals(k) ? Kind.Evt : null;
        if (kind == null) throw corrupt(p, "bad kind");
        Long part = Flat.u64(line, "partition"), parts = Flat.u64(line, "partitions");
        if (part == null || part > 0xFFFFFFFFL) throw corrupt(p, "bad partition");
        if (parts == null || parts > 0xFFFFFFFFL) throw corrupt(p, "bad partitions");
        Long pmin = Flat.i64(line, "pmin"), pmax = Flat.i64(line, "pmax"), mo = Flat.u64(line, "max_orders");
        String ix = Flat.get(line, "index");
        if (pmin == null) throw corrupt(p, "bad pmin");
        if (pmax == null) throw corrupt(p, "bad pmax");
        if (mo == null) throw corrupt(p, "bad max_orders");
        if (!"ladder".equals(ix) && !"tree".equals(ix)) throw corrupt(p, "bad index");
        return new Header(kind, (int) (long) part, (int) (long) parts,
                new Config(pmin, pmax, (int) (long) mo, "tree".equals(ix) ? PriceIndex.Kind.Tree : PriceIndex.Kind.Ladder));
    }

    static Header parseBinaryHeader(Path p, byte[] bytes) {
        if (bytes.length < HEADER || bytes[0] != 'O' || bytes[1] != 'R' || bytes[2] != 'D' || bytes[3] != 'J')
            throw corrupt(p, "bad magic");
        ByteBuffer h = ByteBuffer.wrap(bytes, 0, HEADER).order(ByteOrder.LITTLE_ENDIAN);
        if (h.getShort(4) != 1) throw corrupt(p, "unsupported version");
        Kind kind = bytes[6] == 1 ? Kind.Cmd : bytes[6] == 2 ? Kind.Evt : null;
        if (kind == null) throw corrupt(p, "bad kind");
        if (h.getInt(16) != kind.recordSize) throw corrupt(p, "bad record_size");
        if (bytes[7] < 0 || bytes[7] > 1) throw corrupt(p, "bad index");
        return new Header(kind, h.getInt(8), h.getInt(12), new Config(h.getLong(24), h.getLong(32),
                (int) h.getLong(40), bytes[7] == 1 ? PriceIndex.Kind.Tree : PriceIndex.Kind.Ladder));
    }

    public static Header readHeader(Path p, Format f) {
        byte[] b = readAll(p);
        if (f == Format.Binary) return parseBinaryHeader(p, b);
        String t = new String(b, StandardCharsets.UTF_8);
        int nl = t.indexOf('\n');
        return parseJsonlHeader(p, nl < 0 ? t : t.substring(0, nl));
    }

    static List<String> jsonlLines(Path p, byte[] bytes) {
        String t = new String(bytes, StandardCharsets.UTF_8);
        if (!t.isEmpty() && !t.endsWith("\n")) throw corrupt(p, "torn tail (final line has no newline)");
        List<String> out = new ArrayList<>();
        int pos = 0;
        while (pos < t.length()) {
            int e = t.indexOf('\n', pos);
            out.add(t.substring(pos, e));
            pos = e + 1;
        }
        return out;
    }

    public record CmdFile(Header header, List<CmdRecord> records) {}

    public static CmdFile readCmd(Path p, Format f) {
        byte[] bytes = readAll(p);
        Header h;
        List<CmdRecord> recs = new ArrayList<>();
        if (f == Format.Binary) {
            h = parseBinaryHeader(p, bytes);
            int body = bytes.length - HEADER;
            if (body % CMD_RECORD != 0) throw corrupt(p, "torn tail (partial record)");
            ByteBuffer r = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            r.position(HEADER);
            for (int i = 0; i < body / CMD_RECORD; i++) {
                CmdRecord rec = decodeCmd(r);
                if (rec == null) throw corrupt(p, "record " + i + ": bad codes");
                recs.add(rec);
            }
        } else {
            List<String> lines = jsonlLines(p, bytes);
            h = parseJsonlHeader(p, lines.isEmpty() ? "" : lines.get(0));
            for (int i = 1; i < lines.size(); i++) {
                String l = lines.get(i);
                Long iseq = Flat.u64(l, "iseq"), sym = Flat.u64(l, "symbol");
                Command c = Flat.parseCommand(l);
                if (iseq == null || sym == null || sym < 0 || sym > 0xFFFFFFFFL || c == null)
                    throw corrupt(p, "line " + (i + 1) + ": malformed record: " + l);
                recs.add(new CmdRecord(iseq, sym, c));
            }
        }
        if (h.kind() != Kind.Cmd) throw corrupt(p, "not a command journal");
        for (int i = 1; i < recs.size(); i++)
            if (Long.compareUnsigned(recs.get(i).iseq(), recs.get(i - 1).iseq()) <= 0)
                throw corrupt(p, "iseq not increasing (" + recs.get(i - 1).iseq() + " then " + recs.get(i).iseq() + ")");
        return new CmdFile(h, recs);
    }

    public record CmdDir(Header header, List<List<CmdRecord>> partitions) {}

    public static CmdDir readCmdDir(Path dir, Format f) {
        Path first = path(dir, Kind.Cmd, 0, f);
        CmdFile c0 = readCmd(first, f);
        if (c0.header().partition() != 0) throw corrupt(first, "header partition is not 0");
        List<List<CmdRecord>> all = new ArrayList<>();
        all.add(c0.records());
        for (int p = 1; p < c0.header().partitions(); p++) {
            Path path = path(dir, Kind.Cmd, p, f);
            CmdFile c = readCmd(path, f);
            Header h = c.header();
            if (h.partition() != p || h.partitions() != c0.header().partitions() || !Flat.sameBook(h.book(), c0.header().book()))
                throw corrupt(path, "header does not match its file name, partition count or book config");
            all.add(c.records());
        }
        return new CmdDir(c0.header(), all);
    }

    /// An event journal as canonical symbol-tagged lines.
    public static List<String> readEvt(Path p, Format f) {
        byte[] bytes = readAll(p);
        List<String> out = new ArrayList<>();
        if (f == Format.Binary) {
            parseBinaryHeader(p, bytes);
            int body = bytes.length - HEADER;
            if (body % EVT_RECORD != 0) throw corrupt(p, "torn tail (partial record)");
            ByteBuffer r = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            r.position(HEADER);
            for (int i = 0; i < body / EVT_RECORD; i++) {
                EvtRecord e = decodeEvt(r);
                if (e == null) throw corrupt(p, "record " + i + ": bad codes");
                out.add(Event.canonical(e.seq(), e.sym(), e.ev()));
            }
        } else {
            List<String> lines = jsonlLines(p, bytes);
            parseJsonlHeader(p, lines.isEmpty() ? "" : lines.get(0));
            out.addAll(lines.subList(Math.min(1, lines.size()), lines.size()));
        }
        return out;
    }

    // ---- writing -------------------------------------------------------------------------------

    /// Create (header written) or open for append (header checked); positioned at the end.
    public static FileChannel open(Config2 cfg, Kind k, int p, int P, Config book) throws IOException {
        Path path = path(cfg.dir(), k, p, cfg.format());
        if (cfg.append() && Files.exists(path)) {
            Header want = new Header(k, p, P, book);
            if (!readHeader(path, cfg.format()).same(want)) throw corrupt(path, "header does not match the pipeline");
            return FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        }
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        byte[] h = cfg.format() == Format.Jsonl ? jsonlHeader(k, p, P, book).getBytes(StandardCharsets.UTF_8)
                : binaryHeader(k, p, P, book);
        ByteBuffer hb = ByteBuffer.wrap(h);
        while (hb.hasRemaining()) ch.write(hb);
        return ch;
    }

    /// Asynchronous journal writer: the owner encodes records into a chunk; full
    /// or idle chunks go to a dedicated I/O thread that writes and
    /// group-commits fsyncs (FileChannel.force(false)). Chunks are preallocated and recycled.
    public static final class ChunkWriter {
        static final int CHUNK = 1 << 18, CHUNKS = 64;
        private final FileChannel ch;
        private final FsyncPolicy fsync;
        private final AtomicLong flushed, durable;
        private final ByteBuffer[] chunks = new ByteBuffer[CHUNKS];
        private final long[] lastOf = new long[CHUNKS], recordsOf = new long[CHUNKS];
        private final ArrayBlockingQueue<Integer> toIo = new ArrayBlockingQueue<>(CHUNKS + 2);
        private final ArrayBlockingQueue<Integer> free = new ArrayBlockingQueue<>(CHUNKS + 2);
        private static final Integer STOP = -1;
        private int cur;
        private long last, records;
        private volatile String error;
        private final Thread io;
        private final StringBuilder scratch = new StringBuilder(256);

        public ChunkWriter(FileChannel ch, FsyncPolicy fsync, AtomicLong flushed, AtomicLong durable, String name) {
            this.ch = ch;
            this.fsync = fsync;
            this.flushed = flushed;
            this.durable = durable;
            for (int i = 0; i < CHUNKS; i++) chunks[i] = ByteBuffer.allocateDirect(CHUNK).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 1; i < CHUNKS; i++) free.add(i);
            cur = 0;
            io = new Thread(this::ioLoop, name);
            io.setDaemon(true);
            io.start();
        }

        public ByteBuffer reserve(int len) {
            if (chunks[cur].position() + len > CHUNK) handOff();
            return chunks[cur];
        }
        public void record(long id) { last = id; records++; }
        public int pending() { return chunks[cur].position(); }

        public void pushCmd(Format f, long iseq, long sym, Command c) {
            ByteBuffer b = reserve(MAX_RECORD);
            if (f == Format.Binary) encodeCmd(iseq, sym, c, b);
            else {
                scratch.setLength(0);
                writeCmdLine(iseq, sym, c, scratch);
                scratch.append('\n');
                putAscii(b, scratch);
            }
            record(iseq);
        }

        public void pushEvt(Format f, long seq, long sym, Event e) {
            ByteBuffer b = reserve(MAX_RECORD);
            if (f == Format.Binary) encodeEvt(seq, sym, e, b);
            else putAscii(b, Event.canonical(seq, sym, e) + "\n");
            record(seq);
        }

        private static void putAscii(ByteBuffer b, CharSequence s) {
            for (int i = 0; i < s.length(); i++) b.put((byte) s.charAt(i));
        }

        /// Hand the chunk to the I/O thread (blocks only when every chunk is in flight).
        public void handOff() {
            if (chunks[cur].position() == 0) return;
            lastOf[cur] = last;
            recordsOf[cur] = records;
            records = 0;
            putUninterruptibly(toIo, cur);
            cur = takeUninterruptibly(free);
        }

        /// Write and (per policy) sync everything; the first I/O error, or null.
        public String finish() {
            if (io.isAlive()) {
                handOff();
                putUninterruptibly(toIo, STOP);
                try { io.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                try { ch.close(); } catch (IOException e) { if (error == null) error = e.getMessage(); }
            }
            return error;
        }

        private void write(int c) {
            ByteBuffer b = chunks[c];
            b.flip();
            try { while (b.hasRemaining()) ch.write(b); }
            catch (IOException e) { if (error == null) error = "journal write: " + e.getMessage(); }
            b.clear();
        }

        private void sync(long written) {
            if (error == null) {
                try { ch.force(false); durable.set(written); }
                catch (IOException e) { error = "journal fsync: " + e.getMessage(); }
            }
        }

        private void ioLoop() {
            long unsynced = 0, written = flushed.get(), lastSync = System.nanoTime();
            long idle = fsync == null ? Long.MAX_VALUE : fsync.idleNanos();
            for (;;) {
                Integer m;
                if (unsynced > 0) {
                    try { m = toIo.poll(idle, TimeUnit.NANOSECONDS); } catch (InterruptedException e) { m = null; }
                    if (m == null) { sync(written); unsynced = 0; lastSync = System.nanoTime(); continue; }
                } else {
                    m = takeUninterruptibly(toIo);
                }
                boolean stop = false;
                while (m != null) {  // write everything queued, then one fsync decision
                    if (m.equals(STOP)) { stop = true; break; }
                    write(m);
                    written = lastOf[m];
                    flushed.set(written);
                    unsynced += Math.max(recordsOf[m], 1);
                    putUninterruptibly(free, m);
                    m = toIo.poll();
                }
                boolean due = false;
                if (fsync == null || fsync.mode() == FsyncPolicy.Mode.Never) { durable.set(written); unsynced = 0; }
                else if (fsync.mode() == FsyncPolicy.Mode.EveryN) due = unsynced >= fsync.n();
                else due = System.nanoTime() - lastSync >= fsync.intervalNanos();
                if (due || (stop && unsynced > 0)) { sync(written); unsynced = 0; lastSync = System.nanoTime(); }
                if (stop) return;
            }
        }

        static void putUninterruptibly(ArrayBlockingQueue<Integer> q, Integer v) {
            for (;;) {
                try { q.put(v); return; } catch (InterruptedException ignored) {}
            }
        }
        static int takeUninterruptibly(ArrayBlockingQueue<Integer> q) {
            for (;;) {
                try { return q.take(); } catch (InterruptedException ignored) {}
            }
        }
    }
}
