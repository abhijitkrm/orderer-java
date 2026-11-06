package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.Consensus.Disk;
import io.github.abhijitkrm.orderer.Consensus.Persist;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;

/// The replicated log's write-ahead file, byte-compatible with orderer-rust,
/// orderer-cpp and orderer-go (spec/REPLICATION.md 13.3).
///
/// One consensus write (Persist) is one frame: len u32 | crc32c u32 | body.
/// Recovery cuts a torn tail (including a zero-filled one that a SIGKILL during
/// an append can leave) and refuses to start when valid frames follow a bad one.
public final class Wal implements AutoCloseable {
    static final byte[] MAGIC = {'O', 'R', 'D', 'W', 'A', 'L', 0, 1};
    static final int FRAME_HEAD = 8;
    static final long MAX_FRAME = 256L << 20;
    /// term u32, vote u8, compact flag u8, from u64, entry count u32.
    static final int MIN_BODY = 4 + 1 + 1 + 8 + 4;

    public static final class WalException extends IOException {
        public enum Kind { Io, Corrupt, BadHeader }
        public final Kind kind;
        public final long offset;
        public WalException(Kind k, String m, long off) { super(m); kind = k; offset = off; }
        public WalException(String m) { this(Kind.Io, m, 0); }
    }

    /// What opening a WAL had to cut.
    public record Repair(long frames, long tornBytes) {}

    /// The recovered image and what was repaired.
    public record Opened(Wal wal, Disk disk, Repair repair) {}

    private FileChannel ch;
    private final Path path;
    private long len;
    private Buf buf = new Buf(1 << 16);

    private Wal(FileChannel ch, Path path, long len) { this.ch = ch; this.path = path; this.len = len; }

    static void force(FileChannel c) throws WalException {
        try { c.force(true); } catch (IOException e) { throw new WalException("fsync: " + e.getMessage()); }
    }

    /// Flush a directory entry (best effort: some platforms cannot open directories).
    static void syncDir(Path dir) {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) { d.force(true); }
        catch (IOException ignored) {}
    }

    static void move(Path from, Path to) throws WalException {
        try {
            try { Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
            catch (AtomicMoveNotSupportedException e) { Files.move(from, to, StandardCopyOption.REPLACE_EXISTING); }
        } catch (IOException e) { throw new WalException("rename: " + e.getMessage()); }
    }

    /// Create path with the given contents, fsynced.
    static void writeDurably(Path path, byte[]... parts) throws WalException {
        try (FileChannel c = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (byte[] p : parts) writeAll(c, ByteBuffer.wrap(p));
            force(c);
        } catch (WalException e) { throw e;
        } catch (IOException e) { throw new WalException("write " + path + ": " + e.getMessage()); }
    }

    static void writeAll(FileChannel c, ByteBuffer b) throws IOException {
        while (b.hasRemaining()) c.write(b);
    }

    /// Open (creating if absent) the WAL in dir, repairing a torn tail.
    public static Opened open(Path dir) throws WalException {
        try { Files.createDirectories(dir); } catch (IOException e) { throw new WalException("create " + dir + ": " + e.getMessage()); }
        Path path = dir.resolve("raft.wal"), tmp = dir.resolve("raft.wal.tmp");
        try { Files.deleteIfExists(tmp); } catch (IOException ignored) {}
        if (!Files.exists(path)) {
            writeDurably(tmp, MAGIC);
            move(tmp, path);
            syncDir(dir);
        }
        byte[] data;
        try { data = Files.readAllBytes(path); } catch (IOException e) { throw new WalException("cannot read " + path); }
        Replayed rp = replay(data);
        long torn = data.length - rp.valid;
        try {
            FileChannel c = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            if (torn > 0) { c.truncate(rp.valid); force(c); }
            c.position(rp.valid);
            return new Opened(new Wal(c, path, rp.valid), rp.disk, new Repair(rp.frames, torn));
        } catch (IOException e) { throw new WalException("open " + path + ": " + e.getMessage()); }
    }

    /// Buffer one write. Nothing is durable until sync.
    public void append(Persist p) { frame(p, buf); }

    /// Write buffered frames and fsync (durable=false only writes: benchmarks).
    public void sync(boolean durable) throws WalException {
        try {
            if (buf.len > 0) {
                writeAll(ch, ByteBuffer.wrap(buf.data, 0, buf.len));
                len += buf.len;
                buf.len = 0;
            }
        } catch (IOException e) { throw new WalException("write: " + e.getMessage()); }
        if (durable) force(ch);
    }

    public long length() { return len; }

    /// Replace the file with one frame holding the current state, so a
    /// compacted log's prefix no longer occupies disk. Crash-safe.
    public void rewrite() throws WalException {
        sync(false);
        byte[] data;
        try { data = Files.readAllBytes(path); } catch (IOException e) { throw new WalException("cannot read " + path); }
        Disk disk = replay(data).disk;
        Persist p = new Persist();
        p.term = disk.term;
        p.votedFor = disk.votedFor;
        p.from = disk.base + 1;
        p.entries = disk.log;
        if (disk.base > 0) { p.hasCompact = true; p.compactIndex = disk.base; p.compactTerm = disk.baseTerm; }
        Buf out = new Buf().bytes(MAGIC);
        frame(p, out);
        Path dir = path.getParent(), tmp = dir.resolve("raft.wal.tmp");
        writeDurably(tmp, out.toArray());
        move(tmp, path);
        syncDir(dir);
        try {
            ch.close();
            ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            len = out.len;
            ch.position(len);
        } catch (IOException e) { throw new WalException("reopen " + path + ": " + e.getMessage()); }
    }

    @Override public void close() {
        try { if (ch != null) ch.close(); } catch (IOException ignored) {}
        ch = null;
    }

    static void frame(Persist p, Buf out) {
        int start = out.skip(FRAME_HEAD);
        Codec.encodePersist(p, out);
        int bodyLen = out.len - start - FRAME_HEAD;
        out.setU32(start, bodyLen);
        out.setU32(start + 4, Crc.crc32c(out.data, start + FRAME_HEAD, bodyLen));
    }

    /// Length of the valid frame at off, or -1.
    static int frameAt(byte[] d, int off) {
        if ((long) off + FRAME_HEAD > d.length) return -1;
        long l = Buf.u32(d, off), c = Buf.u32(d, off + 4);
        // A real body is never shorter than the fixed fields of a write. That
        // also rejects zeros: an all-zero header would otherwise pass as valid.
        if (l < MIN_BODY || l > MAX_FRAME || off + FRAME_HEAD + l > d.length) return -1;
        return Crc.crc32c(d, off + FRAME_HEAD, (int) l) == c ? (int) l : -1;
    }

    static boolean fits(Disk disk, Persist p) {
        if (p.from == 0) return false;
        long base = disk.base, n = disk.log.size();
        if (p.hasCompact && p.compactIndex > disk.base) {
            base = p.compactIndex;
            long drop = p.compactIndex - disk.base;
            n = n > drop ? n - drop : 0;
        }
        return p.from > base && p.from - 1 - base <= n;
    }

    record Replayed(Disk disk, int valid, long frames) {}

    /// Replay a whole WAL image. valid is the length of the intact prefix.
    static Replayed replay(byte[] data) throws WalException {
        if (data.length < MAGIC.length || !Arrays.equals(Arrays.copyOf(data, MAGIC.length), MAGIC))
            throw new WalException(WalException.Kind.BadHeader, "wal header missing or unrecognised", 0);
        Disk disk = new Disk();
        disk.log = new ArrayList<>();
        int off = MAGIC.length;
        long frames = 0;
        while (off < data.length) {
            int blen = frameAt(data, off);
            if (blen < 0) {
                // Torn only if nothing valid follows.
                if (validFrameFollows(data, off)) throw new WalException(WalException.Kind.Corrupt, "wal corrupt", off);
                break;
            }
            Persist p;
            try { p = Codec.decodePersist(data, off + FRAME_HEAD, blen); }
            catch (Codec.CodecException e) { throw new WalException(WalException.Kind.Corrupt, "wal corrupt", off); }
            if (!fits(disk, p)) throw new WalException(WalException.Kind.Corrupt, "wal corrupt", off);
            Codec.applyPersist(disk, p);
            off += FRAME_HEAD + blen;
            frames++;
        }
        return new Replayed(disk, off, frames);
    }

    static boolean validFrameFollows(byte[] d, int bad) {
        if ((long) bad + FRAME_HEAD <= d.length) {
            long l = Buf.u32(d, bad);
            if (l >= MIN_BODY && l <= MAX_FRAME) {
                long next = bad + FRAME_HEAD + l;
                if (next < d.length && frameAt(d, (int) next) >= 0) return true;
            }
        }
        // Bounded scan for a frame starting anywhere in the next 1 MiB.
        int end = (int) Math.min(d.length, (long) bad + 1 + (1 << 20));
        for (int o = bad + 1; o < end; o++) {
            if (o + 4 > d.length) break;
            if (Buf.u32(d, o) > d.length - o) continue; // cheap reject before checksumming
            int bl = frameAt(d, o);
            if (bl >= 0) {
                try { Codec.decodePersist(d, o + FRAME_HEAD, bl); return true; } catch (Codec.CodecException ignored) {}
            }
        }
        return false;
    }
}
