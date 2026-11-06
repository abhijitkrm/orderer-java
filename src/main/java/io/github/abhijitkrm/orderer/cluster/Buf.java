package io.github.abhijitkrm.orderer.cluster;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/// A growable little-endian byte buffer, and little-endian accessors on arrays.
public final class Buf {
    static final VarHandle I32 = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    static final VarHandle I64 = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public byte[] data;
    public int len;

    public Buf() { this(64); }
    public Buf(int cap) { data = new byte[Math.max(cap, 16)]; }

    private void room(int n) {
        if (len + n > data.length) data = Arrays.copyOf(data, Math.max(data.length * 2, len + n));
    }

    public Buf u8(int v) { room(1); data[len++] = (byte) v; return this; }
    public Buf u32(long v) { room(4); I32.set(data, len, (int) v); len += 4; return this; }
    public Buf u64(long v) { room(8); I64.set(data, len, v); len += 8; return this; }
    public Buf bool(boolean b) { return u8(b ? 1 : 0); }
    public Buf bytes(byte[] b) { return bytes(b, 0, b.length); }
    public Buf bytes(byte[] b, int off, int n) { room(n); System.arraycopy(b, off, data, len, n); len += n; return this; }
    /// Reserve n zeroed bytes and return their offset.
    public int skip(int n) { room(n); Arrays.fill(data, len, len + n, (byte) 0); int at = len; len += n; return at; }
    public void setU32(int at, long v) { I32.set(data, at, (int) v); }
    public byte[] toArray() { return Arrays.copyOf(data, len); }

    public static long u32(byte[] b, int off) { return (int) I32.get(b, off) & 0xFFFFFFFFL; }
    public static long u64(byte[] b, int off) { return (long) I64.get(b, off); }
    public static void put32(byte[] b, int off, long v) { I32.set(b, off, (int) v); }
    public static void put64(byte[] b, int off, long v) { I64.set(b, off, v); }
}
