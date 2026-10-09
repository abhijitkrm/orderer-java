//! Per-partition journals (spec/JOURNAL.md): naming and segments, JSONL and
//! binary encodings (version 2 records sealed with CRC-32C), strict and
//! repair readers, checkpoints, and the asynchronous chunk writer.
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

public final class Journal {
    private Journal() {}

    public enum Format { Jsonl, Binary }
    public enum Kind {
        Cmd(1, 40), Evt(2, 48);
        final int code, payload;
        Kind(int code, int payload) { this.code = code; this.payload = payload; }
        String label() { return this == Cmd ? "cmd" : "evt"; }
        /// Record size in a binary journal of `version`.
        int recordSize(int version) { return version == 1 ? payload : payload + 8; }
    }

    public static final int HEADER = 64, MAX_RECORD = 256, VERSION = 2;
    /// Version-2 record sizes (1.2): the version-1 record plus CRC-32C and 4 reserved bytes.
    public static final int CMD_RECORD = 48, EVT_RECORD = 56, CMD_RECORD_V1 = 40, EVT_RECORD_V1 = 48;

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

    static String ext(Format f) { return f == Format.Jsonl ? "journal" : "bin"; }

    /// spec/JOURNAL.md §1: segment 0's file.
    public static Path path(Path dir, Kind k, int p, Format f) { return segmentPath(dir, k, p, 0, f); }

    /// spec/JOURNAL.md §1: the segment starting after cut `start`.
    public static Path segmentPath(Path dir, Kind k, int p, long start, Format f) {
        return dir.resolve(k.label() + "-" + p + (start == 0 ? "" : "." + Long.toUnsignedString(start)) + "." + ext(f));
    }

    public record Segment(int partition, long start, Path path) {}

    static Long parseUint(String s) {
        if (s.isEmpty() || s.length() > 19) return null;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) < '0' || s.charAt(i) > '9') return null;
        return Long.parseLong(s);
    }

    /// Every `k` segment in `dir`, sorted by (partition, start).
    public static List<Segment> listSegments(Path dir, Kind k, Format f) {
        List<Segment> out = new ArrayList<>();
        if (!Files.isDirectory(dir)) return out;
        String prefix = k.label() + "-", suffix = "." + ext(f);
        try (var ds = Files.newDirectoryStream(dir)) {
            for (Path e : ds) {
                String name = e.getFileName().toString();
                if (!name.startsWith(prefix) || !name.endsWith(suffix) || name.length() <= prefix.length() + suffix.length())
                    continue;
                String mid = name.substring(prefix.length(), name.length() - suffix.length());
                int dot = mid.indexOf('.');
                Long p = parseUint(dot < 0 ? mid : mid.substring(0, dot));
                Long start = dot < 0 ? Long.valueOf(0) : parseUint(mid.substring(dot + 1));
                if (p == null || p > Integer.MAX_VALUE || start == null || (dot >= 0 && start == 0)) continue;
                out.add(new Segment((int) (long) p, start, e));
            }
        } catch (IOException e) {
            return out;
        }
        out.sort((a, b) -> a.partition != b.partition ? Integer.compare(a.partition, b.partition) : Long.compare(a.start, b.start));
        return out;
    }

    /// spec/JOURNAL.md §6: checkpoint snapshot path for cut `n`.
    public static Path checkpointPath(Path dir, long n) { return dir.resolve("checkpoint-" + n + ".snap"); }

    public record Checkpoint(long cut, Path path) {}

    /// Checkpoints with cut below `below`, ascending; `complete` = with sidecar.
    public static List<Checkpoint> listCheckpoints(Path dir, boolean complete, long below) {
        List<Checkpoint> out = new ArrayList<>();
        if (!Files.isDirectory(dir)) return out;
        try (var ds = Files.newDirectoryStream(dir)) {
            for (Path e : ds) {
                String name = e.getFileName().toString();
                if (!name.startsWith("checkpoint-") || !name.endsWith(".snap")) continue;
                Long n = parseUint(name.substring(11, name.length() - 5));
                if (n == null || n >= below) continue;
                if (complete && !Files.exists(Pipeline.metaPath(e))) continue;
                out.add(new Checkpoint(n, e));
            }
        } catch (IOException e) {
            return out;
        }
        out.sort((a, b) -> Long.compare(a.cut, b.cut));
        return out;
    }

    public static List<Checkpoint> listCheckpoints(Path dir) { return listCheckpoints(dir, true, Long.MAX_VALUE); }

    // ---- encodings ---------------------------------------------------------------------------

    public static String jsonlHeader(Kind k, int p, int P, Config b) {
        return "{\"format\":\"orderer-journal/1\",\"kind\":\"" + k.label() + "\",\"partition\":" + p
                + ",\"partitions\":" + P + ",\"pmin\":" + b.priceMin() + ",\"pmax\":" + b.priceMax()
                + ",\"max_orders\":" + b.maxOrders() + ",\"index\":\"" + Flat.indexName(b.index()) + "\"}\n";
    }

    public static byte[] binaryHeader(Kind k, int p, int P, Config b) {
        ByteBuffer h = ByteBuffer.allocate(HEADER).order(ByteOrder.LITTLE_ENDIAN);
        h.put("ORDJ".getBytes(StandardCharsets.US_ASCII));
        h.putShort((short) VERSION);
        h.put((byte) k.code);
        h.put((byte) (b.index() == PriceIndex.Kind.Tree ? 1 : 0));
        h.putInt(p).putInt(P).putInt(k.recordSize(VERSION)).putInt(0);
        h.putLong(b.priceMin()).putLong(b.priceMax()).putLong(b.maxOrders());
        return h.array();
    }

    /// CRC-32C (Castagnoli) of `len` bytes of `b` from `from`, spec/JOURNAL.md §2.2.
    public static int crc32c(ByteBuffer b, int from, int len) {
        CRC32C c = new CRC32C();
        c.update(b.duplicate().position(from).limit(from + len));
        return (int) c.getValue();
    }

    /// Seal the record of `payload` bytes ending at the buffer's position.
    static void seal(ByteBuffer r, int base, int payload) {
        r.putInt(crc32c(r, base, payload)).putInt(0);
    }

    static boolean sealed(ByteBuffer r, int base, int payload) {
        return r.getInt(base + payload) == crc32c(r, base, payload);
    }

    /// JSONL command record line (no newline): canonical engine line + "iseq".
    public static void writeCmdLine(long iseq, long sym, Command c, StringBuilder out) {
        Flat.writeCommand(c, sym, out);
        out.setLength(out.length() - 1);
        out.append(",\"iseq\":").append(Long.toUnsignedString(iseq)).append('}');
    }

    /// spec/JOURNAL.md §2.2 command record (version 2, sealed).
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
        seal(r, base, CMD_RECORD_V1);
    }

    public record CmdRecord(long iseq, long sym, Command cmd) {}

    /// Decode the version-1 payload at the buffer's position (advances past it).
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

    /// spec/JOURNAL.md §3.2 event record (version 2, sealed): reject 1..7, close 1..3.
    public static void encodeEvt(long seq, long sym, Event e, ByteBuffer r) {
        int base = r.position();
        r.putLong(seq).putInt((int) sym);
        long a = 0, b = 0, c = 0, d = 0;
        int ev, reason = 0;
        if (e instanceof Event.Accepted x) { ev = 1; a = x.orderId(); d = x.leavesQty(); }
        else if (e instanceof Event.Rejected x) { ev = 2; reason = x.reason().ordinal() + 1; a = x.orderId(); }
        else if (e instanceof Event.Trade x) { ev = 3; a = x.maker(); b = x.taker(); c = x.price(); d = x.qty(); }
        else if (e instanceof Event.Closed x) { ev = 4; reason = x.reason().ordinal() + 1; a = x.orderId(); }
        else { Event.Replaced x = (Event.Replaced) e; ev = 5; a = x.orderId(); c = x.price(); d = x.qty(); }
        r.put((byte) ev).put((byte) reason).putShort((short) 0).putLong(a).putLong(b).putLong(c).putLong(d);
        seal(r, base, EVT_RECORD_V1);
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

    /// `version` is the binary journal version (JSONL reports 2); not part of equality.
    public record Header(Kind kind, int partition, int partitions, Config book, int version) {
        boolean same(Header o) {
            return kind == o.kind && partition == o.partition && partitions == o.partitions && Flat.sameBook(book, o.book);
        }
    }

    static CorruptJournal corrupt(Path p, String d) { return new CorruptJournal(p + ": " + d); }

    static byte[] readAll(Path p) {
        try { return Files.readAllBytes(p); }
        catch (IOException e) { throw corrupt(p, String.valueOf(e.getMessage())); }
    }

    static Header parseJsonlHeader(Path p, String line) {
        if (!"orderer-journal/1".equals(Flat.get(line, "format"))) throw corrupt(p, "not an orderer-journal/1 header");
        String k = Flat.get(line, "kind");
        Kind kind = "cmd".equals(k) ? Kind.Cmd : "evt".equals(k) ? Kind.Evt : null;
        if (kind == null) throw corrupt(p, "bad kind");
        Long part = Flat.u64(line, "partition"), parts = Flat.u64(line, "partitions");
        if (part == null || part < 0 || part > 0xFFFFFFFFL) throw corrupt(p, "bad partition");
        if (parts == null || parts < 0 || parts > 0xFFFFFFFFL) throw corrupt(p, "bad partitions");
        Long pmin = Flat.i64(line, "pmin"), pmax = Flat.i64(line, "pmax"), mo = Flat.u64(line, "max_orders");
        String ix = Flat.get(line, "index");
        if (pmin == null) throw corrupt(p, "bad pmin");
        if (pmax == null) throw corrupt(p, "bad pmax");
        if (mo == null) throw corrupt(p, "bad max_orders");
        if (!"ladder".equals(ix) && !"tree".equals(ix)) throw corrupt(p, "bad index");
        return new Header(kind, (int) (long) part, (int) (long) parts,
                new Config(pmin, pmax, (int) (long) mo, "tree".equals(ix) ? PriceIndex.Kind.Tree : PriceIndex.Kind.Ladder), VERSION);
    }

    static Header parseBinaryHeader(Path p, byte[] bytes) {
        if (bytes.length < HEADER || bytes[0] != 'O' || bytes[1] != 'R' || bytes[2] != 'D' || bytes[3] != 'J')
            throw corrupt(p, "bad magic");
        ByteBuffer h = ByteBuffer.wrap(bytes, 0, HEADER).order(ByteOrder.LITTLE_ENDIAN);
        int version = h.getShort(4);
        if (version != 1 && version != 2) throw corrupt(p, "unsupported version");
        Kind kind = bytes[6] == 1 ? Kind.Cmd : bytes[6] == 2 ? Kind.Evt : null;
        if (kind == null) throw corrupt(p, "bad kind");
        if (h.getInt(16) != kind.recordSize(version)) throw corrupt(p, "bad record_size");
        if (bytes[7] < 0 || bytes[7] > 1) throw corrupt(p, "bad index");
        return new Header(kind, h.getInt(8), h.getInt(12), new Config(h.getLong(24), h.getLong(32),
                (int) h.getLong(40), bytes[7] == 1 ? PriceIndex.Kind.Tree : PriceIndex.Kind.Ladder), version);
    }

    public static Header readHeader(Path p, Format f) {
        byte[] b = readAll(p);
        if (f == Format.Binary) return parseBinaryHeader(p, b);
        String t = new String(b, StandardCharsets.UTF_8);
        int nl = t.indexOf('\n');
        return parseJsonlHeader(p, nl < 0 ? t : t.substring(0, nl));
    }

    /// Strict (default) or repair reading (spec/JOURNAL.md §5, §5.1).
    public enum ReadMode { Strict, Repair }

    /// One file's records as byte ranges (binary records already checksum-checked).
    record Body(Header header, int[] offsets, int[] lengths, int validLen) {}

    static Body splitBody(Path p, byte[] bytes, Format f, ReadMode mode) {
        if (f == Format.Binary) {
            Header h = parseBinaryHeader(p, bytes);
            int size = h.kind().recordSize(h.version()), body = bytes.length - HEADER, n = body / size;
            if (body % size != 0 && mode == ReadMode.Strict) throw corrupt(p, "torn tail (partial record)");
            if (h.version() >= 2) {
                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < n; i++) {
                    if (!sealed(bb, HEADER + i * size, h.kind().payload)) {
                        if (mode == ReadMode.Repair && i + 1 == n) { n--; break; }  // a torn final record (§5.1)
                        throw corrupt(p, "record " + i + ": checksum mismatch");
                    }
                }
            }
            int[] off = new int[n], len = new int[n];
            for (int i = 0; i < n; i++) { off[i] = HEADER + i * size; len[i] = size; }
            return new Body(h, off, len, HEADER + n * size);
        }
        int end = bytes.length;
        if (end > 0 && bytes[end - 1] != '\n') {
            if (mode == ReadMode.Strict) throw corrupt(p, "torn tail (final line has no newline)");
            while (end > 0 && bytes[end - 1] != '\n') end--;
        }
        int first = 0;
        while (first < end && bytes[first] != '\n') first++;
        Header h = parseJsonlHeader(p, new String(bytes, 0, Math.min(first, end), StandardCharsets.UTF_8));
        List<int[]> recs = new ArrayList<>();
        for (int pos = first + 1; pos < end;) {
            int e = pos;
            while (bytes[e] != '\n') e++;
            recs.add(new int[] {pos, e - pos});
            pos = e + 1;
        }
        int[] off = new int[recs.size()], len = new int[recs.size()];
        for (int i = 0; i < off.length; i++) { off[i] = recs.get(i)[0]; len[i] = recs.get(i)[1]; }
        return new Body(h, off, len, end);
    }

    static List<CmdRecord> decodeCmds(Path p, byte[] bytes, Format f, Body b) {
        List<CmdRecord> recs = new ArrayList<>(b.offsets().length);
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < b.offsets().length; i++) {
            if (f == Format.Binary) {
                bb.position(b.offsets()[i]);
                CmdRecord rec = decodeCmd(bb);
                if (rec == null) throw corrupt(p, "record " + i + ": bad codes");
                recs.add(rec);
            } else {
                String l = new String(bytes, b.offsets()[i], b.lengths()[i], StandardCharsets.UTF_8);
                Long iseq = Flat.u64(l, "iseq"), sym = Flat.u64(l, "symbol");
                Command c = Flat.parseCommand(l);
                if (iseq == null || sym == null || sym < 0 || sym > 0xFFFFFFFFL || c == null)
                    throw corrupt(p, "line " + (i + 2) + ": malformed record: " + l);
                recs.add(new CmdRecord(iseq, sym, c));
            }
        }
        return recs;
    }

    static void checkIncreasing(Path p, List<CmdRecord> recs, Long after) {
        for (CmdRecord r : recs) {
            if (after != null && Long.compareUnsigned(r.iseq(), after) <= 0)
                throw corrupt(p, "iseq not increasing (" + Long.toUnsignedString(after) + " then " + Long.toUnsignedString(r.iseq()) + ")");
            after = r.iseq();
        }
    }

    public record CmdFile(Header header, List<CmdRecord> records) {}

    /// One command journal file, strictly.
    public static CmdFile readCmd(Path p, Format f) {
        byte[] bytes = readAll(p);
        Body b = splitBody(p, bytes, f, ReadMode.Strict);
        if (b.header().kind() != Kind.Cmd) throw corrupt(p, "not a command journal");
        List<CmdRecord> recs = decodeCmds(p, bytes, f, b);
        checkIncreasing(p, recs, null);
        return new CmdFile(b.header(), recs);
    }

    public record CmdDir(Header header, List<List<CmdRecord>> partitions) {}

    /// Every partition's command journal in `dir`, all segments in order (spec/JOURNAL.md §1, §5).
    public static CmdDir readCmdDir(Path dir, Format f) {
        List<Segment> segs = listSegments(dir, Kind.Cmd, f);
        if (segs.isEmpty()) throw corrupt(path(dir, Kind.Cmd, 0, f), "no command journal");
        Header h0 = readHeader(segs.get(0).path(), f);
        List<List<CmdRecord>> all = new ArrayList<>();
        for (int p = 0; p < h0.partitions(); p++) all.add(new ArrayList<>());
        boolean[] seen = new boolean[h0.partitions()];
        for (Segment s : segs) {
            CmdFile c = readCmd(s.path(), f);
            Header h = c.header();
            if (h.partition() != s.partition() || s.partition() >= h0.partitions() || h.partitions() != h0.partitions()
                    || !Flat.sameBook(h.book(), h0.book()))
                throw corrupt(s.path(), "header does not match its file name, partition count or book config");
            List<CmdRecord> part = all.get(s.partition());
            checkIncreasing(s.path(), c.records(), part.isEmpty() ? null : part.get(part.size() - 1).iseq());
            part.addAll(c.records());
            seen[s.partition()] = true;
        }
        for (int p = 0; p < seen.length; p++)
            if (!seen[p]) throw corrupt(path(dir, Kind.Cmd, p, f), "partition has no journal");
        return new CmdDir(h0, all);
    }

    /// An event journal file as canonical symbol-tagged lines.
    public static List<String> readEvt(Path p, Format f) {
        byte[] bytes = readAll(p);
        Body b = splitBody(p, bytes, f, ReadMode.Strict);
        List<String> out = new ArrayList<>();
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < b.offsets().length; i++) {
            if (f == Format.Binary) {
                bb.position(b.offsets()[i]);
                EvtRecord e = decodeEvt(bb);
                if (e == null) throw corrupt(p, "record " + i + ": bad codes");
                out.add(Event.canonical(e.seq(), e.sym(), e.ev()));
            } else {
                out.add(new String(bytes, b.offsets()[i], b.lengths()[i], StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    /// A partition's whole event journal (all segments, in order).
    public static List<String> readEvtPartition(Path dir, Format f, int partition) {
        List<String> out = new ArrayList<>();
        for (Segment s : listSegments(dir, Kind.Evt, f)) if (s.partition() == partition) out.addAll(readEvt(s.path(), f));
        return out;
    }

    public record Repaired(Path path, long bytes) {}

    /// spec/JOURNAL.md §5.1: truncate a torn tail off each journal family's last segment, in place.
    public static List<Repaired> repairDir(Path dir, Format f) {
        List<Repaired> out = new ArrayList<>();
        for (Kind k : Kind.values()) {
            TreeMap<Integer, Path> last = new TreeMap<>();
            for (Segment s : listSegments(dir, k, f)) last.put(s.partition(), s.path());
            for (Path p : last.values()) {
                byte[] bytes = readAll(p);
                Body b = splitBody(p, bytes, f, ReadMode.Repair);
                if (b.validLen() < bytes.length) {
                    try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
                        ch.truncate(b.validLen());
                        ch.force(true);
                    } catch (IOException e) {
                        throw corrupt(p, String.valueOf(e.getMessage()));
                    }
                    out.add(new Repaired(p, bytes.length - b.validLen()));
                }
            }
        }
        return out;
    }

    // ---- writing -------------------------------------------------------------------------------

    /// Create segment `start` (truncating any old file) with its header; positioned at the end.
    public static FileChannel openSegment(Path dir, Format f, Kind k, int p, int P, Config book, long start) throws IOException {
        FileChannel ch = FileChannel.open(segmentPath(dir, k, p, start, f), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        ByteBuffer hb = ByteBuffer.wrap(f == Format.Jsonl ? jsonlHeader(k, p, P, book).getBytes(StandardCharsets.UTF_8)
                : binaryHeader(k, p, P, book));
        while (hb.hasRemaining()) ch.write(hb);
        return ch;
    }

    /// Append mode: the partition's last segment (header checked); otherwise a fresh segment 0.
    public static FileChannel open(Config2 cfg, Kind k, int p, int P, Config book) throws IOException {
        if (cfg.append()) {
            Path last = null;
            for (Segment s : listSegments(cfg.dir(), k, cfg.format())) if (s.partition() == p) last = s.path();
            if (last != null) {
                Header h = readHeader(last, cfg.format());
                if (!h.same(new Header(k, p, P, book, VERSION))) throw corrupt(last, "header does not match the pipeline");
                if (cfg.format() == Format.Binary && h.version() != VERSION) throw corrupt(last, "cannot append to a version-1 journal");
                return FileChannel.open(last, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            }
        }
        return openSegment(cfg.dir(), cfg.format(), k, p, P, book, 0);
    }

    /// Remove checkpoints with cut below `n` (body first).
    public static void removeCheckpointsBelow(Path dir, long n) throws IOException {
        for (Checkpoint c : listCheckpoints(dir, false, n)) {
            Files.deleteIfExists(c.path());
            Files.deleteIfExists(Pipeline.metaPath(c.path()));
        }
    }

    /// spec/JOURNAL.md §6 step 4: remove segments that start below `n`.
    public static void removeSegmentsBelow(Path dir, Format f, long n) throws IOException {
        for (Kind k : Kind.values())
            for (Segment s : listSegments(dir, k, f)) if (Long.compareUnsigned(s.start(), n) < 0) Files.deleteIfExists(s.path());
    }

    /// A fresh (non-append) pipeline owns its directory's journals.
    public static void clearDir(Path dir, Format f) throws IOException {
        removeSegmentsBelow(dir, f, -1L);
        removeCheckpointsBelow(dir, Long.MAX_VALUE);
    }

    /// Write `contents` durably: temporary name, sync, rename, sync the directory.
    public static void writeDurably(Path path, byte[] contents) throws IOException {
        Path tmp = Path.of(path + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer b = ByteBuffer.wrap(contents);
            while (b.hasRemaining()) ch.write(b);
            ch.force(true);
        }
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Path parent = path.toAbsolutePath().getParent();
        try (FileChannel d = FileChannel.open(parent, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException ignored) {
            // some platforms cannot open a directory for sync
        }
    }

    /// Asynchronous journal writer: the owner encodes records into a chunk; full
    /// or idle chunks go to a dedicated I/O thread that writes and
    /// group-commits fsyncs (FileChannel.force(false)). Chunks are preallocated
    /// and recycled. A rotation (spec/JOURNAL.md §6) rides the same queue, so it
    /// lands between the chunks before it and after it.
    public static final class ChunkWriter {
        static final int CHUNK = 1 << 18, CHUNKS = 64;
        private static final Integer STOP = -1, ROTATE = -2;
        private FileChannel ch;
        private final FsyncPolicy fsync;
        private final AtomicLong flushed, durable;
        private final ByteBuffer[] chunks = new ByteBuffer[CHUNKS];
        private final long[] lastOf = new long[CHUNKS], recordsOf = new long[CHUNKS];
        private final ArrayBlockingQueue<Integer> toIo = new ArrayBlockingQueue<>(CHUNKS + 64);
        private final ArrayBlockingQueue<Integer> free = new ArrayBlockingQueue<>(CHUNKS + 2);
        private final ConcurrentLinkedQueue<FileChannel> rotations = new ConcurrentLinkedQueue<>();
        private int cur;
        private long last, records;
        private volatile String error;
        private final Thread io;
        private final StringBuilder scratch = new StringBuilder(256);

        private Stats.IoStats ioStats = new Stats.IoStats();

        public ChunkWriter(FileChannel ch, FsyncPolicy fsync, AtomicLong flushed, AtomicLong durable, String name) {
            this(ch, fsync, flushed, durable, name, new Stats.IoStats());
        }

        public ChunkWriter(FileChannel ch, FsyncPolicy fsync, AtomicLong flushed, AtomicLong durable, String name,
                           Stats.IoStats ioStats) {
            this.ioStats = ioStats;
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

        /// Continue in `next` (a new segment, header written): everything so far
        /// goes to the current file, which the I/O thread syncs per policy and closes.
        public void rotate(FileChannel next) {
            handOff();
            rotations.add(next);
            putUninterruptibly(toIo, ROTATE);
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
                try {
                    long t0 = System.nanoTime();
                    ch.force(false);
                    ioStats.record(System.nanoTime() - t0);
                    durable.set(written);
                }
                catch (IOException e) { error = "journal fsync: " + e.getMessage(); }
            }
        }

        private boolean syncs() { return fsync != null && fsync.mode() != FsyncPolicy.Mode.Never; }

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
                    if (m.equals(ROTATE)) {
                        if (unsynced > 0 && syncs()) sync(written);
                        unsynced = 0;
                        try { ch.close(); } catch (IOException e) { if (error == null) error = "journal close: " + e.getMessage(); }
                        ch = rotations.poll();
                    } else {
                        write(m);
                        written = lastOf[m];
                        flushed.set(written);
                        unsynced += Math.max(recordsOf[m], 1);
                        putUninterruptibly(free, m);
                    }
                    m = toIo.poll();
                }
                boolean due = false;
                if (!syncs()) { durable.set(written); unsynced = 0; }
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
