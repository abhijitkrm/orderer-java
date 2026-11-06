package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.cluster.Wal.WalException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

/// Durable snapshots, byte-compatible with the other runtimes:
/// snap-<index>.snap = "ORDS" | index u64 | term u32 | len u64 | crc u32 | data,
/// written to a temp name, fsynced, renamed, dir fsynced.
public final class SnapStore {
    static final int HEADER = 4 + 8 + 4 + 8 + 4;

    public record Meta(long index, long term, long len, long crc) {}

    private final Path dir;

    public SnapStore(Path dir) throws WalException {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
            try (Stream<Path> s = Files.list(dir)) {
                for (Path p : (Iterable<Path>) s::iterator)
                    if (p.getFileName().toString().endsWith(".tmp")) Files.deleteIfExists(p);
            }
        } catch (IOException e) { throw new WalException("snapshot dir " + dir + ": " + e.getMessage()); }
    }

    Path path(long index) { return dir.resolve(String.format("snap-%020d.snap", index)); }
    Path tmp(long index) { return dir.resolve(String.format("snap-%020d.tmp", index)); }

    static byte[] header(long index, long term, long len, long crc) {
        Buf h = new Buf(HEADER);
        h.u8('O').u8('R').u8('D').u8('S').u64(index).u32(term).u64(len).u32(crc);
        return h.toArray();
    }

    static Meta parse(byte[] h) {
        if (h.length < HEADER || h[0] != 'O' || h[1] != 'R' || h[2] != 'D' || h[3] != 'S') return null;
        return new Meta(Buf.u64(h, 4), Buf.u32(h, 12), Buf.u64(h, 16), Buf.u32(h, 24));
    }

    /// Store a snapshot durably.
    public void put(long index, long term, byte[] data) throws WalException {
        Path t = tmp(index);
        Wal.writeDurably(t, header(index, term, data.length, Crc.crc32c(data)), data);
        Wal.move(t, path(index));
        Wal.syncDir(dir);
    }

    /// The header, if the file is the right size.
    public Meta meta(long index) {
        Path p = path(index);
        try (RandomAccessFile f = new RandomAccessFile(p.toFile(), "r")) {
            if (f.length() < HEADER) return null;
            byte[] h = new byte[HEADER];
            f.readFully(h);
            Meta m = parse(h);
            return m == null || m.index() != index || f.length() != HEADER + m.len() ? null : m;
        } catch (IOException e) { return null; }
    }

    /// Indexes of intact snapshot files, ascending. A corrupt file is skipped.
    public List<Long> list() {
        List<Long> v = new ArrayList<>();
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                String n = p.getFileName().toString();
                if (n.startsWith("snap-") && n.endsWith(".snap")) {
                    try {
                        long i = Long.parseUnsignedLong(n.substring(5, n.length() - 5));
                        if (meta(i) != null) v.add(i);
                    } catch (NumberFormatException ignored) {}
                }
            }
        } catch (IOException ignored) {}
        v.sort(null);
        return v;
    }

    /// Snapshot contents, verified.
    public record Loaded(Meta meta, byte[] data) {}

    /// Load and verify a snapshot; null if absent or corrupt.
    public Loaded get(long index) {
        byte[] all;
        try { all = Files.readAllBytes(path(index)); } catch (IOException e) { return null; }
        if (all.length < HEADER) return null;
        Meta m = parse(all);
        if (m == null || m.index() != index || all.length - HEADER != m.len()) return null;
        if (Crc.crc32c(all, HEADER, (int) m.len()) != m.crc()) return null;
        return new Loaded(m, Arrays.copyOfRange(all, HEADER, all.length));
    }

    /// Delete snapshots older than keepFrom.
    public void gc(long keepFrom) {
        for (long i : list()) if (i < keepFrom) try { Files.deleteIfExists(path(i)); } catch (IOException ignored) {}
    }

    /// A snapshot being received from a peer, verified before it is published.
    public final class Incoming {
        private FileChannel ch;
        private final Path tmp, dst;
        private final long len, crc;
        private long got;
        private final CRC32C running = new CRC32C();

        private Incoming(long index, long term, long len, long crc) throws WalException {
            this.tmp = SnapStore.this.tmp(index);
            this.dst = path(index);
            this.len = len;
            this.crc = crc;
            try {
                ch = FileChannel.open(tmp, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                Wal.writeAll(ch, ByteBuffer.wrap(header(index, term, len, crc)));
            } catch (IOException e) { abort(); throw new WalException("create " + tmp + ": " + e.getMessage()); }
        }

        public void write(byte[] d, int off, int n) throws WalException {
            if (got + n > len) throw new WalException("snapshot longer than announced");
            got += n;
            running.update(d, off, n);
            try { Wal.writeAll(ch, ByteBuffer.wrap(d, off, n)); } catch (IOException e) { throw new WalException("write: " + e.getMessage()); }
        }

        /// Verify length and checksum and publish the file.
        public void finish() throws WalException {
            if (got != len || running.getValue() != crc) {
                abort();
                throw new WalException(got != len ? "snapshot shorter than announced" : "snapshot checksum mismatch");
            }
            Wal.force(ch);
            try { ch.close(); } catch (IOException ignored) {}
            ch = null;
            Wal.move(tmp, dst);
            Wal.syncDir(dir);
        }

        public void abort() {
            try { if (ch != null) ch.close(); } catch (IOException ignored) {}
            ch = null;
            try { Files.deleteIfExists(tmp); } catch (IOException ignored) {}
        }
    }

    /// Start accepting a snapshot announced by a peer.
    public Incoming receive(long index, long term, long len, long crc) throws WalException {
        return new Incoming(index, term, len, crc);
    }
}
