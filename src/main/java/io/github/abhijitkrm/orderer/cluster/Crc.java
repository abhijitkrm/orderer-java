package io.github.abhijitkrm.orderer.cluster;

import java.util.zip.CRC32C;

/// CRC-32C (Castagnoli); the JDK's implementation is hardware-accelerated.
public final class Crc {
    private Crc() {}

    public static long crc32c(byte[] d, int off, int n) {
        CRC32C c = new CRC32C();
        c.update(d, off, n);
        return c.getValue();
    }

    public static long crc32c(byte[] d) { return crc32c(d, 0, d.length); }
}
