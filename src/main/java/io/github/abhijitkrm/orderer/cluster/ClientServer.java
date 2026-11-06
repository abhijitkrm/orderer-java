package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.cluster.Replica.ProposeResult;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/// Serves one replica's client port, wire-compatible with the other runtimes:
/// requests PROPOSE / STATUS / HASH / BARRIER over the framing of Net. A server
/// holding a cluster key requires the challenge-response handshake. The Replica
/// must outlive the server.
public final class ClientServer {
    static final int Q_HELLO = 0, Q_PROPOSE = 1, Q_STATUS = 2, Q_HASH = 3, Q_BARRIER = 4;
    static final int CR_OK = 1, CR_NOT_LEADER = 2, CR_BUSY = 3, CR_UNKNOWN = 4, CR_ERR = 5, CR_TEXT = 6, CR_HASH = 7, CR_CHALLENGE = 8;

    private final Replica replica;
    private final byte[] key;
    private final ServerSocket listener;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final Map<Socket, Boolean> conns = new ConcurrentHashMap<>();
    private final List<Thread> threads = new ArrayList<>();
    private final Thread acceptor;

    public ClientServer(String addr, Replica replica, byte[] key) throws IOException {
        this.replica = replica;
        this.key = key;
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(Net.addr(addr));
        acceptor = new Thread(() -> {
            while (!stop.get()) {
                Socket c;
                try { c = listener.accept(); }
                catch (IOException e) { if (stop.get()) return; continue; }
                try { c.setTcpNoDelay(true); } catch (IOException ignored) {}
                conns.put(c, true);
                Thread t = new Thread(() -> {
                    try { serve(c); } catch (IOException | RuntimeException ignored) {}
                    finally { conns.remove(c); try { c.close(); } catch (IOException ignored) {} }
                }, "client-conn");
                t.setDaemon(true);
                synchronized (threads) { threads.removeIf(x -> !x.isAlive()); threads.add(t); }
                t.start();
            }
        }, "client-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public void shutdown() {
        if (stop.getAndSet(true)) return;
        try { listener.close(); } catch (IOException ignored) {}
        for (Socket s : conns.keySet()) try { s.close(); } catch (IOException ignored) {}
        try { acceptor.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        List<Thread> ts;
        synchronized (threads) { ts = new ArrayList<>(threads); }
        for (Thread t : ts) try { t.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void reply(OutputStream o, Auth.Session s, int kind, byte[] body) throws IOException { Net.writeFrame(o, kind, body, s); }
    private static void reply(OutputStream o, Auth.Session s, int kind, String text) throws IOException { reply(o, s, kind, text.getBytes(StandardCharsets.UTF_8)); }

    private void serve(Socket conn) throws IOException {
        InputStream in = new BufferedInputStream(conn.getInputStream());
        OutputStream out = conn.getOutputStream();
        Auth.Session sess = null;
        if (key != null) {
            conn.setSoTimeout(5000);
            Net.Frame hello = Net.readFrame(in, null);
            conn.setSoTimeout(0);
            if (hello == null || hello.kind() != Q_HELLO || hello.body().length != Auth.NONCE_LEN) return;
            byte[] ns = Auth.nonce();
            byte[] mac = Auth.handshake(key, "cchal", hello.body(), ns);
            Net.writeFrame(out, CR_CHALLENGE, new Buf(32).bytes(ns).bytes(mac, 0, Auth.MAC_LEN).toArray(), null);
            sess = new Auth.Session(Auth.handshake(key, "csess", hello.body(), ns), 1);
        }
        for (;;) {
            Net.Frame f = Net.readFrame(in, sess);
            if (f == null) return;
            switch (f.kind()) {
                case Q_HELLO -> { reply(out, sess, CR_ERR, "this server does not use authentication"); return; }
                case Q_PROPOSE -> answer(out, sess, replica.propose(f.body()));
                case Q_BARRIER -> answer(out, sess, replica.barrier());
                case Q_STATUS -> {
                    Replica.Status s = replica.status();
                    reply(out, sess, CR_TEXT, String.format(
                            "id=%d role=%s term=%d leader=%d commit=%d applied=%d base=%d last=%d pending=%d halted=%s digests_verified=%d\n",
                            s.id(), s.role().label, s.term(), s.leader(), s.commit(), s.applied(), s.base(), s.lastIndex(),
                            s.pending(), s.halted().isEmpty() ? "-" : s.halted(), s.digestsVerified()));
                }
                case Q_HASH -> {
                    Replica.Hash h = replica.stateHash();
                    if (h != null) reply(out, sess, CR_HASH, new Buf(40).u64(h.index()).bytes(h.digest()).toArray());
                    else reply(out, sess, CR_ERR, "state hash unsupported");
                }
                default -> reply(out, sess, CR_ERR, "unknown request");
            }
        }
    }

    private static void answer(OutputStream out, Auth.Session sess, ProposeResult r) throws IOException {
        switch (r.kind()) {
            case Ok -> reply(out, sess, CR_OK, new Buf(8).u64(r.index()).toArray());
            case NotLeader -> reply(out, sess, CR_NOT_LEADER, new byte[] {(byte) (r.hint() < 0 ? 255 : r.hint())});
            case Stopped -> reply(out, sess, CR_NOT_LEADER, new byte[] {(byte) 255});
            case Busy -> reply(out, sess, CR_BUSY, new byte[0]);
            default -> reply(out, sess, CR_UNKNOWN, "unknown outcome");
        }
    }

}
