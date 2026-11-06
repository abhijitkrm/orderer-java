package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.orderer.cluster.Transport.Member;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/// The client side of the client protocol: a leader-following Client with
/// exactly-once sessions, over Conn connections to each replica's client port.
public final class Client implements AutoCloseable {
    public enum Kind {
        /// Committed on a quorum.
        Ok,
        /// No leader could be reached in time.
        NoLeader,
        /// The outcome is unknown; it may still commit. Never retry blindly (use a session).
        Unknown,
        /// A malformed or unexpected reply.
        Protocol
    }

    public record Result(Kind kind, long index, String text) {
        public boolean ok() { return kind == Kind.Ok; }
    }

    /// A connection to one replica's client port.
    public static final class Conn implements AutoCloseable {
        private final Socket sock;
        private final InputStream in;
        private final OutputStream out;
        private final Auth.Session sess;

        private Conn(Socket s, InputStream in, OutputStream out, Auth.Session sess) { sock = s; this.in = in; this.out = out; this.sess = sess; }

        /// Open a connection, authenticating when key is set; null on failure.
        public static Conn connect(String addr, byte[] key) {
            Socket s = null;
            try {
                s = Net.connect(addr, 500);
                s.setSoTimeout(30_000);
                InputStream in = new BufferedInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                Auth.Session sess = null;
                if (key != null) {
                    byte[] nc = Auth.nonce();
                    Net.writeFrame(out, ClientServer.Q_HELLO, nc, null);
                    Net.Frame f = Net.readFrame(in, null);
                    if (f == null || f.kind() != ClientServer.CR_CHALLENGE || f.body().length != Auth.NONCE_LEN + Auth.MAC_LEN) { s.close(); return null; }
                    byte[] ns = java.util.Arrays.copyOf(f.body(), Auth.NONCE_LEN);
                    byte[] want = Auth.handshake(key, "cchal", nc, ns);
                    if (!Auth.ctEq(want, 0, f.body(), Auth.NONCE_LEN, Auth.MAC_LEN)) { s.close(); return null; }
                    sess = new Auth.Session(Auth.handshake(key, "csess", nc, ns), 0);
                }
                return new Conn(s, in, out, sess);
            } catch (IOException e) {
                if (s != null) try { s.close(); } catch (IOException ignored) {}
                return null;
            }
        }

        /// null on any I/O or protocol failure.
        Net.Frame call(int kind, byte[] body) {
            try {
                Net.writeFrame(out, kind, body, sess);
                return Net.readFrame(in, sess);
            } catch (IOException e) { return null; }
        }

        public String status() {
            Net.Frame f = call(ClientServer.Q_STATUS, new byte[0]);
            return f == null || f.kind() != ClientServer.CR_TEXT ? null : new String(f.body(), StandardCharsets.UTF_8);
        }

        /// A linearizable read barrier (this node must be the leader); -1 on failure.
        public long barrier() {
            Net.Frame f = call(ClientServer.Q_BARRIER, new byte[0]);
            return f == null || f.kind() != ClientServer.CR_OK || f.body().length != 8 ? -1 : Buf.u64(f.body(), 0);
        }

        /// The node's (applied index, state digest), or null.
        public Replica.Hash hash() {
            Net.Frame f = call(ClientServer.Q_HASH, new byte[0]);
            if (f == null || f.kind() != ClientServer.CR_HASH || f.body().length != 40) return null;
            return new Replica.Hash(Buf.u64(f.body(), 0), java.util.Arrays.copyOfRange(f.body(), 8, 40));
        }

        @Override public void close() { try { sock.close(); } catch (IOException ignored) {} }
    }

    private final List<Member> addrs;
    private final Conn[] conns;
    private int current;
    private byte[] key;
    private long sessionId, sessionSeq;
    private boolean hasSession;
    private byte[] pending;
    /// Bounds proposeSession's retries; default 30 s.
    public long retryBudgetMs = 30_000;

    public Client(List<Member> addrs) { this.addrs = new ArrayList<>(addrs); this.conns = new Conn[addrs.size()]; }

    public Client withKey(byte[] key) { this.key = key; return this; }

    /// Exactly-once submission under session id (one sequential stream per id).
    public Client withSession(long id) { sessionId = id; sessionSeq = 0; hasSession = true; return this; }

    @Override public void close() {
        for (int i = 0; i < conns.length; i++) if (conns[i] != null) { conns[i].close(); conns[i] = null; }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /// Submit an encoded payload. Ok = committed on a quorum. Unknown = may still commit.
    public Result propose(byte[] payload) {
        for (int attempt = 0; attempt < 600; attempt++) {
            int i = current % addrs.size();
            if (conns[i] == null) conns[i] = Conn.connect(addrs.get(i).addr(), key);
            Conn c = conns[i];
            if (c == null) { current++; sleep(10); continue; }
            Net.Frame f = c.call(ClientServer.Q_PROPOSE, payload);
            if (f == null) {
                // Died mid-request: it may have been accepted. Unknown, never a blind retry.
                c.close(); conns[i] = null; current++;
                return new Result(Kind.Unknown, 0, "connection lost");
            }
            switch (f.kind()) {
                case ClientServer.CR_OK -> {
                    if (f.body().length != 8) return new Result(Kind.Protocol, 0, "bad reply");
                    return new Result(Kind.Ok, Buf.u64(f.body(), 0), "");
                }
                case ClientServer.CR_NOT_LEADER -> {
                    int pos = addrs.size();
                    if (f.body().length > 0) for (int k = 0; k < addrs.size(); k++) if (addrs.get(k).id() == (f.body()[0] & 0xFF)) pos = k;
                    if (pos < addrs.size()) current = pos; else current++;
                    sleep(5);
                }
                case ClientServer.CR_BUSY -> sleep(2);
                case ClientServer.CR_UNKNOWN -> {
                    c.close(); conns[i] = null;
                    return new Result(Kind.Unknown, 0, new String(f.body(), StandardCharsets.UTF_8));
                }
                default -> { return new Result(Kind.Protocol, 0, "reply " + f.kind()); }
            }
        }
        return new Result(Kind.NoLeader, 0, "");
    }

    /// Submit a batch with exactly-once effect: the same (session, seq) is
    /// retried through lost connections and leader changes until it is
    /// definitely committed.
    public Result proposeSession(long[] syms, Command[] cmds) {
        if (!hasSession) return new Result(Kind.Protocol, 0, "no session: call withSession first");
        if (pending == null) {
            sessionSeq++;
            pending = Codec.encodeBatch(syms, cmds, new Codec.SessionKey(sessionId, sessionSeq));
        }
        long deadline = System.nanoTime() + retryBudgetMs * 1_000_000L;
        for (;;) {
            Result r = propose(pending);
            if (r.ok()) { pending = null; return r; }
            if (r.kind() == Kind.Protocol || System.nanoTime() >= deadline) return r;
            sleep(50);
        }
    }

    /// Parse "ID=HOST:PORT,ID=HOST:PORT,...".
    public static List<Member> parseMembers(String s) {
        List<Member> out = new ArrayList<>();
        for (String m : s.split(",")) {
            int eq = m.indexOf('=');
            if (eq < 0) throw new IllegalArgumentException("want ID=HOST:PORT, got " + m);
            out.add(new Member(Integer.parseInt(m.substring(0, eq)), m.substring(eq + 1).replace("localhost", "127.0.0.1")));
        }
        return out;
    }
}
