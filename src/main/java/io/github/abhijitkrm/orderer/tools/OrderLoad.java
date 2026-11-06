package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.cluster.Buf;
import io.github.abhijitkrm.orderer.cluster.Client;
import io.github.abhijitkrm.orderer.cluster.Transport.Member;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/// orderload: load generator, cluster inspector and failover verifier (the
/// Java port of orderer-rust's orderer-load; same modes, flags and output).
///
///   orderload run    --clients A,B,C --secs S --acked FILE [--maybe FILE] [--batch N] [--threads T]
///   orderload status --clients A,B,C
///   orderload settle --clients A,B,C [--timeout S]
///   orderload verify --acked FILE [--maybe FILE] --applied F1,F2,...
///
/// run writes the id of every acknowledged batch to the acked file as soon as it
/// is acknowledged, and ids with an unknown outcome to the maybe file. verify
/// then proves that nothing acknowledged was lost, nothing was applied twice or
/// invented, and every replica applied the same sequence. ORDERER_KEY in the
/// environment sets the pre-shared cluster key.
public final class OrderLoad {
    private OrderLoad() {}

    static void die(String m) { System.err.println("orderload: " + m); System.exit(2); }

    static byte[] key() {
        String k = System.getenv("ORDERER_KEY");
        return k == null || k.isEmpty() ? null : k.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    static List<Member> addrs(String s) {
        List<Member> out = new ArrayList<>();
        String[] parts = s.split(",");
        for (int i = 0; i < parts.length; i++) {
            if (!parts[i].contains(":")) die("bad address " + parts[i]);
            out.add(new Member(i, parts[i]));
        }
        return out;
    }

    static List<Long> readIds(String path) {
        List<Long> out = new ArrayList<>();
        try { for (String l : Files.readAllLines(Path.of(path))) try { out.add(Long.parseUnsignedLong(l.trim())); } catch (NumberFormatException ignored) {} }
        catch (IOException ignored) {}
        return out;
    }

    record Applied(long index, long id) {}

    static List<Applied> readApplied(String path) {
        byte[] b = null;
        try { b = Files.readAllBytes(Path.of(path)); } catch (IOException e) { die(path + ": " + e.getMessage()); }
        List<Applied> out = new ArrayList<>();
        for (int i = 0; i + 16 <= b.length; i += 16) out.add(new Applied(Buf.u64(b, i), Buf.u64(b, i + 8)));
        return out;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) die("mode: run|status|settle|verify");
        String mode = args[0], clients = "", acked = "", maybe = "", applied = "";
        long secs = 10, timeout = 60;
        int batch = 50, threads = 2;
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--clients" -> clients = args[++i];
                case "--secs" -> secs = Long.parseLong(args[++i]);
                case "--acked" -> acked = args[++i];
                case "--maybe" -> maybe = args[++i];
                case "--applied" -> applied = args[++i];
                case "--batch" -> batch = Integer.parseInt(args[++i]);
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--timeout" -> timeout = Long.parseLong(args[++i]);
                default -> die("unknown argument " + args[i]);
            }
        }
        switch (mode) {
            case "run" -> run(addrs(clients), secs, acked, maybe, batch, threads);
            case "status" -> status(addrs(clients));
            case "settle" -> settle(addrs(clients), timeout);
            case "verify" -> verify(acked, maybe, applied);
            default -> die("unknown mode " + mode);
        }
    }

    static void run(List<Member> clients, long secs, String acked, String maybe, int batch, int threads) throws Exception {
        if (acked.isEmpty()) die("--acked is required");
        PrintWriter ackedF = new PrintWriter(new FileWriter(acked, true), true);
        PrintWriter maybeF = maybe.isEmpty() ? null : new PrintWriter(new FileWriter(maybe, true), true);
        long end = System.nanoTime() + secs * 1_000_000_000L;
        long[] counts = new long[3];
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tt = t;
            Thread th = new Thread(() -> {
                // One session per thread: retries are exactly-once.
                Client c = new Client(clients).withSession(tt + 1);
                byte[] k = key();
                if (k != null) c.withKey(k);
                long x = 0x9E3779B97F4A7C15L ^ (tt + 1);
                long seq = 0;
                while (System.nanoTime() < end) {
                    seq++;
                    // The batch id doubles as its first order's id; the rest are derived from it so every order id is unique.
                    long id = (((long) (tt + 1) << 40) | seq) << 12;
                    long[] syms = new long[batch];
                    Command[] cmds = new Command[batch];
                    for (int j = 0; j < batch; j++) {
                        x ^= x << 13; x ^= x >>> 7; x ^= x << 17;
                        Side side = (x & 1) == 0 ? Side.Bid : Side.Ask;
                        long price = 10_000 + Long.remainderUnsigned(x >>> 8, 8) - 4;
                        syms[j] = Long.remainderUnsigned(x, 16);
                        cmds[j] = Command.newLimit(id | j, side, price, 1 + Long.remainderUnsigned(x >>> 20, 10), Tif.Gtc);
                    }
                    Client.Result r = c.proposeSession(syms, cmds);
                    synchronized (counts) {
                        switch (r.kind()) {
                            case Ok -> { ackedF.println(Long.toUnsignedString(id)); counts[0]++; }
                            case Unknown -> { if (maybeF != null) maybeF.println(Long.toUnsignedString(id)); counts[1]++; }
                            default -> counts[2]++;
                        }
                    }
                }
                c.close();
            }, "load-" + t);
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) th.join();
        ackedF.close();
        if (maybeF != null) maybeF.close();
        System.out.printf("load finished: %d acknowledged, %d unknown, %d not submitted (%d commands each)%n", counts[0], counts[1], counts[2], batch);
    }

    static void status(List<Member> clients) {
        for (Member a : clients) {
            Client.Conn c = Client.Conn.connect(a.addr(), key());
            if (c == null) { System.out.println(a.addr() + ": unreachable (connect failed)"); continue; }
            String s = c.status();
            if (s != null) System.out.print(a.addr() + ": " + s);
            else System.out.println(a.addr() + ": unreachable (no status)");
            c.close();
        }
    }

    static String field(String s, String k) {
        for (String kv : s.trim().split("\\s+")) if (kv.startsWith(k + "=")) return kv.substring(k.length() + 1);
        return "";
    }

    /// Wait until every node has applied everything committed and all agree.
    static void settle(List<Member> clients, long timeout) throws Exception {
        long end = System.nanoTime() + timeout * 1_000_000_000L;
        for (;;) {
            List<String> stats = new ArrayList<>();
            List<io.github.abhijitkrm.orderer.cluster.Replica.Hash> hashes = new ArrayList<>();
            boolean ok = true;
            for (Member a : clients) {
                Client.Conn c = Client.Conn.connect(a.addr(), key());
                if (c == null) { ok = false; continue; }
                String s = c.status();
                var h = c.hash();
                c.close();
                if (s == null || h == null) { ok = false; continue; }
                stats.add(s); hashes.add(h);
            }
            if (ok && stats.size() == clients.size()) {
                for (String s : stats) {
                    String h = field(s, "halted");
                    if (!h.isEmpty() && !h.equals("-")) { System.out.println("FAIL: a node halted"); System.exit(1); }
                }
                boolean agree = true;
                for (int i = 0; i < stats.size(); i++) {
                    if (!field(stats.get(i), "applied").equals(field(stats.get(0), "applied"))
                            || !field(stats.get(i), "commit").equals(field(stats.get(0), "commit"))
                            || !field(stats.get(i), "applied").equals(field(stats.get(i), "commit"))
                            || hashes.get(i).index() != hashes.get(0).index()
                            || !java.util.Arrays.equals(hashes.get(i).digest(), hashes.get(0).digest())) agree = false;
                }
                if (agree) {
                    System.out.printf("settled: all %d nodes at index %d, state digest %s%n", clients.size(), hashes.get(0).index(), HexFormat.of().formatHex(hashes.get(0).digest()));
                    return;
                }
            }
            if (System.nanoTime() > end) {
                System.out.println("FAIL: nodes did not settle within " + timeout + "s");
                status(clients);
                System.exit(1);
            }
            Thread.sleep(200);
        }
    }

    static void verify(String acked, String maybe, String appliedPaths) {
        List<Long> ackedIds = readIds(acked);
        Set<Long> maybeSet = new HashSet<>(readIds(maybe)), ackedSet = new HashSet<>(ackedIds);
        if (ackedSet.size() != ackedIds.size()) { System.out.println("FAIL: the load generator acknowledged an id twice"); System.exit(1); }
        List<Applied> first = null;
        int failures = 0, i = 0;
        for (String path : appliedPaths.split(",")) {
            List<Applied> log = readApplied(path);
            Set<Long> set = new HashSet<>();
            for (Applied l : log) set.add(l.id());
            List<String> bad = new ArrayList<>();
            if (set.size() != log.size()) bad.add("a batch was applied twice");
            for (int k = 1; k < log.size(); k++) if (Long.compareUnsigned(log.get(k).index(), log.get(k - 1).index()) <= 0) { bad.add("log indexes are not increasing"); break; }
            long missing = ackedSet.stream().filter(a -> !set.contains(a)).count();
            if (missing > 0) bad.add(missing + " acknowledged batches are missing");
            long invented = set.stream().filter(s -> !ackedSet.contains(s) && !maybeSet.contains(s)).count();
            if (invented > 0) bad.add(invented + " applied batches were never submitted");
            if (i++ == 0) first = log;
            else if (!first.equals(log)) bad.add("applied sequence differs from the first node's");
            if (bad.isEmpty()) System.out.printf("%s: ok (%d batches applied, %d acknowledged all present)%n", path, log.size(), ackedSet.size());
            else { failures++; System.out.printf("%s: FAIL: %s%n", path, String.join("; ", bad)); }
        }
        if (failures > 0) System.exit(1);
        System.out.println("PASS: every acknowledged batch survived on every replica, in the same order, exactly once");
    }
}
