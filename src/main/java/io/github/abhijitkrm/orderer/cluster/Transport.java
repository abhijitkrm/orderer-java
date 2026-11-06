package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.Consensus.Msg;
import io.github.abhijitkrm.orderer.cluster.Codec.Reader;
import io.github.abhijitkrm.orderer.cluster.Wal.WalException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/// TCP transport between replicas, wire-compatible with the other runtimes.
/// Sends never block the protocol: each peer has a bounded queue and a thread
/// that reconnects with backoff; a full queue or a down peer drops messages,
/// which the protocol tolerates. Snapshots stream in chunks and are verified by
/// length and checksum before they are accepted. With a cluster key, every
/// connection runs the challenge-response handshake of Auth and every frame is
/// MAC'd.
public final class Transport {
    static final int K_MSG = 1, K_SNAP_BEGIN = 2, K_SNAP_CHUNK = 3, K_SNAP_END = 4, K_DIGEST = 5, K_HELLO = 6,
            K_CHALLENGE = 7, K_AUTH = 8;
    static final int SNAP_CHUNK = 1 << 20;
    static final int PEER_QUEUE = 8192;

    /// What the transport hands to the node.
    public static final class Inbound {
        public enum Kind { Msg, Snapshot, Digest }
        public Kind kind;
        public int from;
        public Msg msg;
        public long term, lastTerm, index;
        public byte[] digest;
    }

    /// Transport activity counters.
    public static final class Stats {
        public final AtomicLong sent = new AtomicLong(), dropped = new AtomicLong(), received = new AtomicLong(), badFrames = new AtomicLong();
    }

    /// One replica's id and peer address ("host:port").
    public record Member(int id, String addr) {}

    private record Job(int kind, Msg msg, long term, long lastTerm, long index, byte[] digest) {}   // kind: 0 msg, 1 snapshot, 2 digest

    private static final class Peer {
        final String addr;
        final BlockingQueue<Job> q = new ArrayBlockingQueue<>(PEER_QUEUE);
        Thread thread;
        Peer(String a) { addr = a; }
    }

    public final Stats stats = new Stats();
    private final int me;
    private final List<Integer> ids = new ArrayList<>();
    private final long clusterId;
    private final Consumer<Inbound> sink;
    private final SnapStore snaps;
    private final Filter filter;
    private final byte[] key;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final ServerSocket listener;
    private final Thread acceptor;
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    private final Map<Socket, Boolean> conns = new ConcurrentHashMap<>();
    private final List<Thread> readers = new ArrayList<>();

    public Transport(int me, List<Member> members, long clusterId, Consumer<Inbound> sink, SnapStore snaps, Filter filter, byte[] key) throws IOException {
        this.me = me; this.clusterId = clusterId; this.sink = sink; this.snaps = snaps; this.filter = filter; this.key = key;
        String mine = null;
        for (Member m : members) { ids.add(m.id()); if (m.id() == me) mine = m.addr(); }
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(Net.addr(mine));
        acceptor = new Thread(this::acceptLoop, "transport-accept-" + me);
        acceptor.setDaemon(true);
        acceptor.start();
        for (Member m : members) {
            if (m.id() == me) continue;
            Peer p = new Peer(m.addr());
            p.thread = new Thread(() -> senderLoop(m.id(), p), "transport-send-" + me + "-" + m.id());
            p.thread.setDaemon(true);
            peers.put(m.id(), p);
            p.thread.start();
        }
    }

    private void enqueue(int to, Job j) {
        if (!filter.allows(me, to)) { stats.dropped.incrementAndGet(); return; }
        Peer p = peers.get(to);
        if (p == null) return;
        if (!p.q.offer(j)) stats.dropped.incrementAndGet();
    }

    public void send(int to, Msg m) { enqueue(to, new Job(0, m, 0, 0, 0, null)); }
    /// Stream the stored snapshot at index to a peer.
    public void sendSnapshot(int to, long term, long index, long lastTerm) { enqueue(to, new Job(1, null, term, lastTerm, index, null)); }
    public void sendDigest(int to, long index, byte[] digest) { enqueue(to, new Job(2, null, 0, 0, index, digest)); }

    public void shutdown() {
        if (stop.getAndSet(true)) return;
        try { listener.close(); } catch (IOException ignored) {}
        for (Socket s : conns.keySet()) try { s.close(); } catch (IOException ignored) {}
        join(acceptor);
        for (Peer p : peers.values()) { p.thread.interrupt(); join(p.thread); }
        List<Thread> rs;
        synchronized (readers) { rs = new ArrayList<>(readers); }
        for (Thread t : rs) join(t);
    }

    private static void join(Thread t) {
        try { t.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private byte[] helloBody() { return new Buf(9).u8(me).u64(clusterId).toArray(); }

    // ---- outbound ----

    private Auth.Session initiatorHandshake(Socket s, InputStream in, OutputStream out, int peer) throws IOException {
        byte[] ns = Auth.nonce();
        byte[] h = helloBody();
        byte[] hello = new byte[h.length + ns.length];
        System.arraycopy(h, 0, hello, 0, h.length);
        System.arraycopy(ns, 0, hello, h.length, ns.length);
        Net.writeFrame(out, K_HELLO, hello, null);
        s.setSoTimeout(2000);
        Net.Frame f = Net.readFrame(in, null);
        s.setSoTimeout(0);
        if (f == null || f.kind() != K_CHALLENGE || f.body().length != Auth.NONCE_LEN + Auth.MAC_LEN) return null;
        byte[] nr = java.util.Arrays.copyOf(f.body(), Auth.NONCE_LEN);
        byte[] ids = {(byte) me, (byte) peer};
        byte[] want = Auth.handshake(key, "chal", ns, nr, ids);
        if (!Auth.ctEq(want, 0, f.body(), Auth.NONCE_LEN, Auth.MAC_LEN)) return null;
        Auth.Session sess = new Auth.Session(Auth.handshake(key, "sess", ns, nr, ids), 0);
        Net.writeFrame(out, K_AUTH, new byte[0], sess);   // proves this side knows the key too
        return sess;
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { if (!stop.get()) Thread.currentThread().interrupt(); }
    }

    private void senderLoop(int id, Peer p) {
        long backoff = 20;
        while (!stop.get()) {
            Socket s;
            try { s = Net.connect(p.addr, 300); }
            catch (IOException e) {
                int n = p.q.size();
                p.q.clear();   // shed what queued meanwhile
                stats.dropped.addAndGet(n);
                sleep(backoff);
                backoff = Math.min(backoff * 2, 500);
                continue;
            }
            backoff = 20;
            conns.put(s, true);
            if (stop.get()) { try { s.close(); } catch (IOException ignored) {} return; }
            try { sendOn(id, p, s); }
            catch (IOException ignored) {}   // connection lost: reconnect
            finally { conns.remove(s); try { s.close(); } catch (IOException ignored) {} }
        }
    }

    private void sendOn(int id, Peer p, Socket s) throws IOException {
        InputStream in = new BufferedInputStream(s.getInputStream());
        OutputStream out = s.getOutputStream();
        Auth.Session sess = null;
        if (key != null) {
            sess = initiatorHandshake(s, in, out, id);
            if (sess == null) { stats.badFrames.incrementAndGet(); sleep(200); return; }
        } else Net.writeFrame(out, K_HELLO, helloBody(), null);
        Buf buf = new Buf(1 << 16);
        while (!stop.get()) {
            Job j;
            try { j = p.q.poll(50, TimeUnit.MILLISECONDS); }
            catch (InterruptedException e) { return; }
            while (j != null) {
                if (filter.allows(me, id)) {
                    stats.sent.incrementAndGet();
                    writeJob(out, buf, sess, j);
                    if (buf.len > (256 << 10)) { out.write(buf.data, 0, buf.len); buf.len = 0; }
                }
                j = p.q.poll();
            }
            if (buf.len > 0) { out.write(buf.data, 0, buf.len); out.flush(); buf.len = 0; }
        }
    }

    private void writeJob(OutputStream out, Buf buf, Auth.Session sess, Job j) throws IOException {
        switch (j.kind()) {
            case 0 -> {
                Buf b = Codec.encodeMsg(j.msg(), new Buf(Codec.encodedSize(j.msg())));
                Net.putFrame(buf, K_MSG, b.data, 0, b.len, sess);
            }
            case 2 -> {
                Buf b = new Buf(40).u64(j.index()).bytes(j.digest());
                Net.putFrame(buf, K_DIGEST, b.toArray(), sess);
            }
            default -> streamSnapshot(out, buf, sess, j);
        }
    }

    private void streamSnapshot(OutputStream out, Buf buf, Auth.Session sess, Job j) throws IOException {
        SnapStore.Loaded snap = snaps.get(j.index());
        if (snap == null) return;   // gone (superseded): the peer will ask again
        Buf b = new Buf(32).u32(j.term()).u64(j.index()).u32(j.lastTerm()).u64(snap.meta().len()).u32(snap.meta().crc());
        Net.putFrame(buf, K_SNAP_BEGIN, b.toArray(), sess);
        byte[] data = snap.data();
        for (int at = 0; at < data.length; at += SNAP_CHUNK) {
            Net.putFrame(buf, K_SNAP_CHUNK, data, at, Math.min(SNAP_CHUNK, data.length - at), sess);
            out.write(buf.data, 0, buf.len);
            buf.len = 0;
        }
        Net.putFrame(buf, K_SNAP_END, new byte[0], sess);
    }

    // ---- inbound ----

    private void acceptLoop() {
        while (!stop.get()) {
            Socket c;
            try { c = listener.accept(); }
            catch (IOException e) { if (stop.get()) return; sleep(10); continue; }
            try { c.setTcpNoDelay(true); } catch (IOException ignored) {}
            conns.put(c, true);
            Thread t = new Thread(() -> {
                try { readerLoop(c); }
                catch (IOException | RuntimeException ignored) {}
                finally { conns.remove(c); try { c.close(); } catch (IOException ignored) {} }
            }, "transport-read-" + me);
            t.setDaemon(true);
            synchronized (readers) { readers.removeIf(x -> !x.isAlive()); readers.add(t); }
            t.start();
            if (stop.get()) try { c.close(); } catch (IOException ignored) {}
        }
    }

    private void readerLoop(Socket conn) throws IOException {
        InputStream in = new BufferedInputStream(conn.getInputStream());
        OutputStream out = conn.getOutputStream();
        conn.setSoTimeout(5000);
        Net.Frame first = Net.readFrame(in, null);
        conn.setSoTimeout(0);
        if (first == null) return;
        int want = key != null ? 9 + Auth.NONCE_LEN : 9;
        if (first.kind() != K_HELLO || first.body().length != want) { stats.badFrames.incrementAndGet(); return; }
        int from = first.body()[0] & 0xFF;
        if (Buf.u64(first.body(), 1) != clusterId || from == me || !ids.contains(from)) { stats.badFrames.incrementAndGet(); return; }
        Auth.Session sess = null;
        if (key != null) {
            byte[] ns = java.util.Arrays.copyOfRange(first.body(), 9, 9 + Auth.NONCE_LEN);
            byte[] nr = Auth.nonce();
            byte[] ids = {(byte) from, (byte) me};
            byte[] mac = Auth.handshake(key, "chal", ns, nr, ids);
            byte[] reply = new Buf(32).bytes(nr).bytes(mac, 0, Auth.MAC_LEN).toArray();
            Net.writeFrame(out, K_CHALLENGE, reply, null);
            sess = new Auth.Session(Auth.handshake(key, "sess", ns, nr, ids), 1);
            conn.setSoTimeout(2000);
            Net.Frame a = Net.readFrame(in, sess);
            conn.setSoTimeout(0);
            if (a == null || a.kind() != K_AUTH || a.body().length != 0) { stats.badFrames.incrementAndGet(); return; }
        }
        SnapStore.Incoming incoming = null;
        long inTerm = 0, inLast = 0, inIndex = 0;
        try {
            for (;;) {
                Net.Frame f;
                try { f = Net.readFrame(in, sess); }
                catch (Net.FrameException e) { stats.badFrames.incrementAndGet(); throw e; }
                if (f == null) return;
                if (!filter.allows(from, me)) continue;
                stats.received.incrementAndGet();
                Reader r = new Reader(f.body());
                try {
                    switch (f.kind()) {
                        case K_MSG -> {
                            Msg m = Codec.decodeMsg(r);
                            r.finish();
                            Inbound i = new Inbound(); i.kind = Inbound.Kind.Msg; i.from = from; i.msg = m;
                            sink.accept(i);
                        }
                        case K_DIGEST -> {
                            Inbound i = new Inbound(); i.kind = Inbound.Kind.Digest; i.from = from; i.index = r.u64();
                            i.digest = r.bytes(32, true);
                            sink.accept(i);
                        }
                        case K_SNAP_BEGIN -> {
                            inTerm = r.u32(); inIndex = r.u64(); inLast = r.u32();
                            long len = r.u64(), crc = r.u32();
                            if (incoming != null) incoming.abort();
                            incoming = null;
                            incoming = snaps.receive(inIndex, inLast, len, crc);
                        }
                        case K_SNAP_CHUNK -> {
                            if (incoming == null) { stats.badFrames.incrementAndGet(); return; }
                            incoming.write(f.body(), 0, f.body().length);
                        }
                        case K_SNAP_END -> {
                            if (incoming == null) { stats.badFrames.incrementAndGet(); return; }
                            SnapStore.Incoming done = incoming;
                            incoming = null;
                            done.finish();
                            Inbound i = new Inbound(); i.kind = Inbound.Kind.Snapshot; i.from = from; i.term = inTerm;
                            i.index = inIndex; i.lastTerm = inLast;
                            sink.accept(i);
                        }
                        default -> { stats.badFrames.incrementAndGet(); return; }
                    }
                } catch (Codec.CodecException e) { stats.badFrames.incrementAndGet(); return; }
                  catch (WalException e) { return; }
            }
        } finally {
            if (incoming != null) incoming.abort();
        }
    }
}
