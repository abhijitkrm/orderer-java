package io.github.abhijitkrm.orderer.cluster;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

/// TCP helpers and the framing shared by the replica transport and the client
/// protocol: len u32 | crc32c u32 | kind u8 | body [| 16-byte MAC once the
/// connection is authenticated]. len and crc cover kind, body and MAC.
/// Byte-compatible with the other runtimes.
public final class Net {
    private Net() {}

    static final int MAX_NET_FRAME = 64 << 20;

    /// A framing, checksum or authentication failure on a connection.
    public static final class FrameException extends IOException {
        public FrameException(String m) { super(m); }
    }

    public record Frame(int kind, byte[] body) {}

    /// Append one frame (sealed when sess is set) to out without sending.
    public static void putFrame(Buf out, int kind, byte[] body, int off, int n, Auth.Session sess) {
        int start = out.skip(8);
        out.u8(kind);
        out.bytes(body, off, n);
        if (sess != null) out.bytes(sess.seal(kind, body, off, n));
        int payload = out.len - start - 8;
        out.setU32(start, payload);
        out.setU32(start + 4, Crc.crc32c(out.data, start + 8, payload));
    }

    public static void putFrame(Buf out, int kind, byte[] body, Auth.Session sess) {
        putFrame(out, kind, body, 0, body.length, sess);
    }

    public static void writeFrame(OutputStream o, int kind, byte[] body, Auth.Session sess) throws IOException {
        Buf b = new Buf(body.length + 32);
        putFrame(b, kind, body, sess);
        o.write(b.data, 0, b.len);
        o.flush();
    }

    private static boolean readFull(InputStream in, byte[] b, int n, boolean atBoundary) throws IOException {
        int got = 0;
        while (got < n) {
            int r = in.read(b, got, n - got);
            if (r < 0) {
                if (got == 0 && atBoundary) return false;
                throw new EOFException("unexpected eof");
            }
            got += r;
        }
        return true;
    }

    /// Read one frame, verifying length, checksum and (with sess) the MAC.
    /// null means a clean close at a frame boundary.
    public static Frame readFrame(InputStream in, Auth.Session sess) throws IOException {
        byte[] head = new byte[8];
        if (!readFull(in, head, 8, true)) return null;
        long n = Buf.u32(head, 0), crc = Buf.u32(head, 4);
        if (n == 0 || n > MAX_NET_FRAME) throw new FrameException("frame length");
        byte[] body = new byte[(int) n];
        readFull(in, body, (int) n, false);
        if (Crc.crc32c(body) != crc) throw new FrameException("frame checksum");
        int kind = body[0] & 0xFF;
        int blen = body.length - 1;
        if (sess != null) {
            if (blen < Auth.MAC_LEN) throw new FrameException("frame authentication");
            blen -= Auth.MAC_LEN;
            if (!sess.open(kind, body, 1, blen, body, 1 + blen)) throw new FrameException("frame authentication");
        }
        return new Frame(kind, Arrays.copyOfRange(body, 1, 1 + blen));
    }

    /// Parse "host:port" (host a name or address).
    public static InetSocketAddress addr(String s) {
        int c = s.lastIndexOf(':');
        if (c < 0) throw new IllegalArgumentException("bad address " + s);
        String host = s.substring(0, c);
        return new InetSocketAddress(host.equals("localhost") ? "127.0.0.1" : host, Integer.parseInt(s.substring(c + 1)));
    }

    public static Socket connect(String addr, int timeoutMs) throws IOException {
        Socket s = new Socket();
        try {
            s.connect(addr(addr), timeoutMs);
            s.setTcpNoDelay(true);
            return s;
        } catch (IOException e) { try { s.close(); } catch (IOException ignored) {} throw e; }
    }
}
