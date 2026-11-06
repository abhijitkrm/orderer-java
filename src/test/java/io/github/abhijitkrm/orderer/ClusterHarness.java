// Test harness: real replicas over TCP with real files.
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.orderer.Consensus.Entry;
import io.github.abhijitkrm.orderer.Consensus.Role;
import io.github.abhijitkrm.orderer.cluster.Buf;
import io.github.abhijitkrm.orderer.cluster.DedupSm;
import io.github.abhijitkrm.orderer.cluster.RecorderSm;
import io.github.abhijitkrm.orderer.cluster.Replica;
import io.github.abhijitkrm.orderer.cluster.StateMachine;
import io.github.abhijitkrm.orderer.cluster.Transport.Member;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

final class ClusterHarness {
    private ClusterHarness() {}

    static byte[] payload(long i) { return new Buf(8).u64(i).toArray(); }
    static long idOf(byte[] p) { return Buf.u64(p, 0); }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { throw new RuntimeException(e); }
    }

    static void rmrf(Path p) {
        try (var s = Files.walk(p)) { s.sorted(java.util.Comparator.reverseOrder()).forEach(x -> x.toFile().delete()); }
        catch (IOException ignored) {}
    }

    static List<String> freeAddrs(int n) throws IOException {
        List<ServerSocket> ss = new ArrayList<>();
        List<String> out = new ArrayList<>();
        for (int i = 0; i < n; i++) { ServerSocket s = new ServerSocket(0); ss.add(s); out.add("127.0.0.1:" + s.getLocalPort()); }
        for (ServerSocket s : ss) s.close();
        return out;
    }

    /// A RecorderSm whose state the test can read while the applier runs.
    static final class Shared {
        final RecorderSm sm = new RecorderSm();
        volatile long corruptAt = -1;
        final AtomicLong applyDelayMs = new AtomicLong();

        synchronized List<byte[]> payloads() {
            List<byte[]> v = new ArrayList<>();
            for (var r : sm.applied()) v.add(r.data());
            return v;
        }
        int count() { return payloads().size(); }
    }

    static final class Probe implements StateMachine {
        final Shared s;
        Probe(Shared s) { this.s = s; }
        @Override public void apply(long i, Entry e) {
            long d = s.applyDelayMs.get();
            if (d > 0) sleep(d);
            synchronized (s) {
                if (s.corruptAt == i && e.data().length > 0) {
                    byte[] c = e.data().clone();
                    c[0] ^= (byte) 0xff;
                    e = new Entry(e.term(), c);
                }
                s.sm.apply(i, e);
            }
        }
        @Override public byte[] snapshot(long i) { synchronized (s) { return s.sm.snapshot(i); } }
        @Override public void restore(long i, byte[] d) { synchronized (s) { s.sm.restore(i, d); } }
        @Override public byte[] digest(long i) { synchronized (s) { return s.sm.digest(i); } }
    }

    static boolean same(List<byte[]> a, List<byte[]> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) if (!Arrays.equals(a.get(i), b.get(i))) return false;
        return true;
    }

    static List<byte[]> seq(long from, long to) {
        List<byte[]> v = new ArrayList<>();
        for (long i = from; i < to; i++) v.add(payload(i));
        return v;
    }

    interface SmFactory { StateMachine make(int node, Shared shared, List<AutoCloseable> closers); }

    static final class Cluster implements AutoCloseable {
        final Path dir;
        final List<Member> members = new ArrayList<>();
        final Replica[] nodes;
        final Shared[] shared;
        final Consumer<Replica.Config> tune;
        final SmFactory smf;
        final List<AutoCloseable> closers = new ArrayList<>();

        Cluster(int n, Consumer<Replica.Config> tune, boolean dedup) throws IOException {
            this(n, tune, (i, sh, cl) -> dedup ? new DedupSm(new Probe(sh)) : new Probe(sh));
        }

        Cluster(int n, Consumer<Replica.Config> tune, SmFactory smf) throws IOException {
            this.dir = Files.createTempDirectory("orderer-java-e2e");
            this.tune = tune;
            this.smf = smf;
            nodes = new Replica[n];
            shared = new Shared[n];
            List<String> addrs = freeAddrs(n);
            for (int i = 0; i < n; i++) { members.add(new Member(i, addrs.get(i))); shared[i] = new Shared(); }
            for (int i = 0; i < n; i++) start(i);
        }

        Replica.Config config(int i) {
            Replica.Config c = new Replica.Config(i, members, dir.resolve("n" + i));
            c.tickMs = 5;
            c.consensus = new Consensus.Config(20, 2, 256, true);
            c.fsync = false;
            c.proposeTimeoutMs = 3000;
            if (tune != null) tune.accept(c);
            return c;
        }

        void start(int i) throws IOException {
            StateMachine sm = smf.make(i, shared[i], closers);
            nodes[i] = Replica.start(config(i), sm, null);
        }

        void crash(int i) { nodes[i].crash(); nodes[i] = null; }
        void stop(int i) { nodes[i].stop(); nodes[i] = null; }
        void restart(int i) throws IOException { shared[i] = new Shared(); start(i); }

        @Override public void close() {
            for (Replica r : nodes) if (r != null) r.crash();
            for (AutoCloseable c : closers) try { c.close(); } catch (Exception ignored) {}
            rmrf(dir);
        }

        int leader() {
            int best = -1;
            long bt = 0;
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i] == null) continue;
                Replica.Status s = nodes[i].status();
                if (s.role() == Role.LEADER && (best < 0 || s.term() > bt)) { best = i; bt = s.term(); }
            }
            return best;
        }

        int waitLeader(long withinMs) {
            long end = System.currentTimeMillis() + withinMs;
            for (;;) {
                int l = leader();
                if (l >= 0) return l;
                if (System.currentTimeMillis() > end) { T.check(false, "no leader within ", withinMs, "ms"); return 0; }
                sleep(10);
            }
        }

        /// Propose via whichever node leads, retrying redirects. Timeout and Unknown are returned.
        Replica.ProposeResult propose(byte[] p) {
            long end = System.currentTimeMillis() + 10_000;
            for (;;) {
                int l = leader();
                if (l < 0) {
                    if (System.currentTimeMillis() > end) return new Replica.ProposeResult(Replica.Kind.Stopped, 0, -1);
                    sleep(10);
                    continue;
                }
                Replica.ProposeResult r = nodes[l].propose(p);
                if (r.kind() == Replica.Kind.NotLeader || r.kind() == Replica.Kind.Busy || r.kind() == Replica.Kind.Stopped) {
                    if (System.currentTimeMillis() > end) return r;
                    sleep(5);
                    continue;
                }
                return r;
            }
        }

        void mustPropose(byte[] p) {
            Replica.ProposeResult r = propose(p);
            T.check(r.ok(), "proposal failed: ", r);
        }

        void waitApplied(int[] who, int atLeast, long withinMs) {
            long end = System.currentTimeMillis() + withinMs;
            for (;;) {
                boolean ok = true;
                for (int i : who) ok &= shared[i].count() >= atLeast;
                if (ok) return;
                if (System.currentTimeMillis() > end) { T.check(false, "nodes did not apply ", atLeast); return; }
                sleep(10);
            }
        }

        /// Wait until every running node has applied the same, stable sequence.
        List<byte[]> waitConverged(long withinMs) {
            long end = System.currentTimeMillis() + withinMs;
            for (;;) {
                List<List<byte[]>> a = snap();
                if (allSame(a)) {
                    sleep(100);
                    List<List<byte[]>> b = snap();
                    if (allSame(b) && (a.isEmpty() || same(a.get(0), b.get(0)))) return a.isEmpty() ? List.of() : a.get(0);
                }
                if (System.currentTimeMillis() > end) { T.check(false, "replicas did not converge"); return List.of(); }
                sleep(20);
            }
        }

        private List<List<byte[]>> snap() {
            List<List<byte[]>> v = new ArrayList<>();
            for (int i = 0; i < nodes.length; i++) if (nodes[i] != null) v.add(shared[i].payloads());
            return v;
        }

        private static boolean allSame(List<List<byte[]>> v) {
            for (List<byte[]> x : v) if (!same(x, v.get(0))) return false;
            return true;
        }
    }
}
