package io.github.abhijitkrm.orderer.cluster;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/// Pre-shared-key authentication: a challenge-response handshake proves both
/// sides know the cluster key, and every later frame carries a MAC over
/// (direction, sequence, kind, body). That gives authentication and integrity,
/// not encryption. Same construction as orderer-rust's auth.rs.
public final class Auth {
    private Auth() {}

    public static final int MAC_LEN = 16, NONCE_LEN = 16;
    private static final SecureRandom RNG = new SecureRandom();

    static Mac mac(byte[] key) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key.length == 0 ? new byte[1] : key, "HmacSHA256"));
            return m;
        } catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
    }

    public static byte[] hmac(byte[] key, byte[]... parts) {
        Mac m = mac(key);
        for (byte[] p : parts) m.update(p);
        return m.doFinal();
    }

    /// HMAC(key, label | parts...).
    public static byte[] handshake(byte[] key, String label, byte[]... parts) {
        byte[][] all = new byte[parts.length + 1][];
        all[0] = label.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        System.arraycopy(parts, 0, all, 1, parts.length);
        return hmac(key, all);
    }

    public static byte[] nonce() {
        byte[] n = new byte[NONCE_LEN];
        RNG.nextBytes(n);
        return n;
    }

    public static boolean ctEq(byte[] a, int ao, byte[] b, int bo, int n) {
        if (a.length - ao < n || b.length - bo < n) return false;
        int acc = 0;
        for (int i = 0; i < n; i++) acc |= a[ao + i] ^ b[bo + i];
        return acc == 0;
    }

    /// MACs the frames of one authenticated connection (one per side).
    public static final class Session {
        private final Mac mac;
        private final int sendDir;
        private long sendSeq, recvSeq;

        public Session(byte[] key, int sendDir) { this.mac = Auth.mac(key); this.sendDir = sendDir; }

        private byte[] tag(int dir, long seq, int kind, byte[] body, int off, int n) {
            byte[] head = new byte[10];
            head[0] = (byte) dir;
            Buf.put64(head, 1, seq);
            head[9] = (byte) kind;
            mac.update(head);
            mac.update(body, off, n);
            return mac.doFinal();
        }

        public synchronized byte[] seal(int kind, byte[] body, int off, int n) {
            byte[] t = tag(sendDir, sendSeq++, kind, body, off, n);
            return java.util.Arrays.copyOf(t, MAC_LEN);
        }

        /// Verify a frame; a rejection does not advance the sequence.
        public synchronized boolean open(int kind, byte[] body, int off, int n, byte[] tagBuf, int tagOff) {
            byte[] want = tag(1 - sendDir, recvSeq, kind, body, off, n);
            if (!ctEq(want, 0, tagBuf, tagOff, MAC_LEN)) return false;
            recvSeq++;
            return true;
        }
    }

    static boolean digestEq(byte[] a, byte[] b) { return MessageDigest.isEqual(a, b); }
}
