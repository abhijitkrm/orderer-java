package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.Consensus;
import io.github.abhijitkrm.orderer.Consensus.Entry;
import io.github.abhijitkrm.orderer.cluster.Codec;
import io.github.abhijitkrm.orderer.cluster.PipelineSm;
import io.github.abhijitkrm.orderer.cluster.Replica;
import io.github.abhijitkrm.orderer.cluster.ShardMap;
import io.github.abhijitkrm.orderer.cluster.StateMachine;
import io.github.abhijitkrm.orderer.cluster.Transport.Member;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/// clusterbench: throughput of the replicated path, in one process over
/// loopback TCP (the Java counterpart of orderer-rust's clusterbench, same flags
/// and same output).
///
///   clusterbench [--nodes N] [--groups G] [--sm noop|pipeline] [--partitions P]
///                [--batch CMDS] [--producers K] [--window W]
///                [--cmds TOTAL] [--fsync on|off] [--dir PATH] [--pmax PRICE]
///
/// G independent replicated groups of N nodes each (symbols sharded between
/// them). For every group, K producers keep W batches of CMDS commands in
/// flight against its leader. Everything shares one machine: use the numbers as
/// a floor for what dedicated hardware per replica would do.
///
/// Java's price ladder holds one Level object per price tick, about 70 MB per
/// book at the default 0..1,000,000 range, so three replicas of 64 symbols do not
/// fit in memory. The benchmark's price range is --pmax (default 20000, the
/// workload trades around 10000); the nodes of a real cluster keep the default
/// range, which the snapshot header and so the state digest depend on.
public final class ClusterBench {
    private ClusterBench() {}

    static Path workdir;

    static final class Noop implements StateMachine {
        public void apply(long i, Entry e) {}
        public byte[] snapshot(long i) { return new byte[0]; }
        public void restore(long i, byte[] d) {}
    }

    static void die(String m) {
        System.err.println(m);
        if (workdir != null) deleteTree(workdir);
        System.exit(2);
    }

    static void deleteTree(Path p) {
        try (var s = Files.walk(p)) { s.sorted(Comparator.reverseOrder()).forEach(x -> x.toFile().delete()); }
        catch (IOException ignored) {}
    }

    public static void main(String[] args) throws Exception {
        int nodes = 3, groups = 1, partitions = 2, batch = 1000, producers = 2, window = 8;
        long total = 20_000_000L, pmax = 20_000;
        String sm = "noop", dir = "";
        boolean fsync = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nodes" -> nodes = Integer.parseInt(args[++i]);
                case "--groups" -> groups = Integer.parseInt(args[++i]);
                case "--sm" -> sm = args[++i];
                case "--partitions" -> partitions = Integer.parseInt(args[++i]);
                case "--batch" -> batch = Integer.parseInt(args[++i]);
                case "--producers" -> producers = Integer.parseInt(args[++i]);
                case "--window" -> window = Integer.parseInt(args[++i]);
                case "--cmds" -> total = Long.parseLong(args[++i]);
                case "--fsync" -> fsync = args[++i].equals("on");
                case "--dir" -> dir = args[++i];
                case "--pmax" -> pmax = Long.parseLong(args[++i]);
                default -> die("unknown argument " + args[i]);
            }
        }
        Path root = dir.isEmpty() ? Path.of(System.getProperty("java.io.tmpdir"), "clusterbench-java-" + ProcessHandle.current().pid()) : Path.of(dir);
        deleteTree(root);
        workdir = root;

        final long fPmax = pmax;
        List<List<Replica>> all = new ArrayList<>();
        List<PipelineSm<?>> sms = new ArrayList<>();
        for (int g = 0; g < groups; g++) {
            List<Member> members = new ArrayList<>();
            List<ServerSocket> ls = new ArrayList<>();
            for (int i = 0; i < nodes; i++) {
                ServerSocket s = new ServerSocket(0);
                ls.add(s);
                members.add(new Member(i, "127.0.0.1:" + s.getLocalPort()));
            }
            for (ServerSocket s : ls) s.close();
            List<Replica> reps = new ArrayList<>();
            for (int i = 0; i < nodes; i++) {
                Replica.Config cfg = new Replica.Config(i, members, root.resolve("g" + g + "n" + i));
                cfg.clusterId = 1000 + g;
                cfg.consensus = new Consensus.Config(30, 3, 512, true);
                cfg.fsync = fsync;
                cfg.maxPending = 1 << 20;
                cfg.proposeTimeoutMs = 60_000;
                StateMachine m;
                switch (sm) {
                    case "noop" -> m = new Noop();
                    case "pipeline" -> { PipelineSm<?> p = PipelineSm.counting(partitions, new OrderBook.Config(0, fPmax, 65_536, io.github.abhijitkrm.matcher.PriceIndex.Kind.Ladder)); sms.add(p); m = p; }
                    default -> { die("--sm " + sm); return; }
                }
                reps.add(Replica.start(cfg, m, null));
            }
            all.add(reps);
        }

        int[] leaders = new int[groups];
        long endWait = System.nanoTime() + 20_000_000_000L;
        for (int g = 0; g < groups; g++) {
            leaders[g] = -1;
            while (leaders[g] < 0) {
                for (int i = 0; i < nodes; i++) if (all.get(g).get(i).status().role() == Consensus.Role.LEADER) leaders[g] = i;
                if (leaders[g] < 0) {
                    if (System.nanoTime() > endWait) die("no leader");
                    Thread.sleep(10);
                }
            }
        }

        ShardMap map = new ShardMap(groups);
        long perProducer = total / ((long) groups * producers);
        AtomicLong acked = new AtomicLong(), latNs = new AtomicLong(), batches = new AtomicLong();
        final int fBatch = batch, fWindow = window;
        long t0 = System.nanoTime();
        List<Thread> threads = new ArrayList<>();
        for (int g = 0; g < groups; g++) {
            Replica leader = all.get(g).get(leaders[g]);
            List<Long> owned = new ArrayList<>();   // symbols this group owns
            for (long s = 0; s < 4096 && owned.size() < 64; s++) if (map.groupOf(s) == g) owned.add(s);
            long[] syms = owned.stream().mapToLong(Long::longValue).toArray();
            for (int p = 0; p < producers; p++) {
                final int gg = g, pp = p;
                Thread th = new Thread(() -> {
                    long nextId = ((long) (gg * 64 + pp + 1)) << 36;
                    long x = 0x9E3779B97F4A7C15L ^ (gg * 64 + pp);
                    long sent = 0;
                    record Flight(long t, CompletableFuture<Replica.ProposeResult> f) {}
                    ArrayDeque<Flight> q = new ArrayDeque<>();
                    long[] bs = new long[fBatch];
                    Command[] cmds = new Command[fBatch];
                    while (sent < perProducer || !q.isEmpty()) {
                        while (sent < perProducer && q.size() < fWindow) {
                            for (int i = 0; i < fBatch; i++) {
                                x ^= x << 13; x ^= x >>> 7; x ^= x << 17;
                                nextId++;
                                Side side = (x & 1) == 0 ? Side.Bid : Side.Ask;
                                bs[i] = syms[(int) Long.remainderUnsigned(x, syms.length)];
                                cmds[i] = Command.newLimit(nextId, side, 10_000 + Long.remainderUnsigned(x >>> 8, 8) - 4, 1 + Long.remainderUnsigned(x >>> 20, 10), Tif.Gtc);
                            }
                            byte[] payload = Codec.encodeBatch(bs, cmds, null);
                            q.add(new Flight(System.nanoTime(), leader.proposeAsync(payload)));
                            sent += fBatch;
                        }
                        Flight h = q.poll();
                        if (h == null) break;
                        Replica.ProposeResult r = h.f().join();
                        switch (r.kind()) {
                            case Ok -> { acked.addAndGet(fBatch); latNs.addAndGet(System.nanoTime() - h.t()); batches.incrementAndGet(); }
                            case Busy -> { try { Thread.sleep(1); } catch (InterruptedException e) { return; } }
                            default -> die("proposal failed: " + r);
                        }
                    }
                }, "producer-" + g + "-" + p);
                threads.add(th);
                th.start();
            }
        }
        for (Thread th : threads) th.join();
        double secs = (System.nanoTime() - t0) / 1e9;
        long done = acked.get(), nb = Math.max(batches.get(), 1);
        // Every node of every group must apply what its group committed.
        long deadline = System.nanoTime() + 180_000_000_000L;
        for (int g = 0; g < groups; g++) {
            long want = all.get(g).get(leaders[g]).status().commit();
            for (;;) {
                boolean caught = true;
                for (Replica r : all.get(g)) if (r.status().applied() < want) caught = false;
                if (caught) break;
                if (System.nanoTime() > deadline) die("followers did not catch up");
                Thread.sleep(5);
            }
        }
        double appliedSecs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("groups=%d nodes=%d sm=%s batch=%d producers=%d/group window=%d fsync=%s: %.2fM cmds/s acked (%.2fM per group), "
                        + "%.2fM applied everywhere, %.0f MB/s log payload, mean batch latency %.2f ms, %d cmds in %.2fs%n",
                groups, nodes, sm, batch, producers, window, fsync ? "on" : "off", done / secs / 1e6, done / secs / 1e6 / groups,
                done / appliedSecs / 1e6, done * 32.0 / 1e6 / secs, latNs.get() / (double) nb / 1e6, done, secs);
        for (List<Replica> rs : all) for (Replica r : rs) r.crash();
        for (PipelineSm<?> p : sms) p.close();
        deleteTree(root);
        System.exit(0);
    }
}
