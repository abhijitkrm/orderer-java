// The matching pipeline as the replicated state machine: replicas must hold
// byte-identical state, including after a snapshot of real book state is
// installed and after a restart from disk.
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.ClusterHarness.*;
import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.matcher.PriceIndex;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.cluster.Codec;
import io.github.abhijitkrm.orderer.cluster.DedupSm;
import io.github.abhijitkrm.orderer.cluster.PipelineSm;
import io.github.abhijitkrm.orderer.cluster.Replica;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

public final class ClusterPipelineTest {
    // A price range that covers the workload (about 10000): Java's ladder holds one object per tick.
    static final OrderBook.Config BOOK = new OrderBook.Config(0, 20_000, 65_536, PriceIndex.Kind.Ladder);

    static Cluster pipeCluster(int n, Consumer<Replica.Config> tune) throws Exception {
        return new Cluster(n, tune, (i, sh, cl) -> {
            PipelineSm<?> p = PipelineSm.counting(2, BOOK);
            cl.add(p::close);
            return new DedupSm(p);
        });
    }

    /// n commands over a few symbols: limits on both sides that cross, some cancels.
    static byte[] randomBatch(Random rng, long[] next, int n) {
        long[] syms = new long[n];
        Command[] cmds = new Command[n];
        for (int i = 0; i < n; i++) {
            next[0]++;
            syms[i] = 1 + rng.nextInt(8);
            if (rng.nextInt(10) == 0 && next[0] > 20) cmds[i] = Command.cancel(next[0] - 1 - rng.nextInt(15));
            else cmds[i] = Command.newLimit(next[0], rng.nextBoolean() ? Side.Bid : Side.Ask, 10000 + rng.nextInt(9) - 4, 1 + rng.nextInt(9), Tif.Gtc);
        }
        return Codec.encodeBatch(syms, cmds, null);
    }

    /// Waits for every running replica to apply `want` and returns their digests.
    static List<byte[]> hashes(Cluster c, long want, long withinMs) {
        long end = System.currentTimeMillis() + withinMs;
        for (;;) {
            List<byte[]> out = new ArrayList<>();
            boolean ok = true;
            for (Replica r : c.nodes) {
                if (r == null) continue;
                Replica.Hash h = r.stateHash();
                if (h == null || h.index() < want) { ok = false; break; }
                check(h.index() == want, "applied ", h.index(), " past the expected ", want);
                out.add(h.digest());
            }
            if (ok) return out;
            if (System.currentTimeMillis() > end) { check(false, "replicas did not reach the expected index"); return out; }
            sleep(20);
        }
    }

    static void allEqual(List<byte[]> h, String what) {
        for (int i = 0; i < h.size(); i++) check(Arrays.equals(h.get(i), h.get(0)), "replica ", i, " ", what);
    }

    public static void main(String[] args) {
        T.runAll(new Object[][] {
            {"pipeline_replicas_hold_identical_state", (T.Body) () -> {
                try (Cluster c = pipeCluster(3, cfg -> cfg.digestEvery = 32)) {
                    c.waitLeader(5000);
                    Random rng = new Random(1);
                    long[] next = {0};
                    Replica.ProposeResult last = null;
                    for (int i = 0; i < 200; i++) {
                        last = c.propose(randomBatch(rng, next, 20));
                        check(last.ok(), "batch ", i, ": ", last);
                    }
                    allEqual(hashes(c, last.index(), 10_000), "state differs");
                    long end = System.currentTimeMillis() + 5000;
                    while (c.nodes[0].status().digestsVerified() == 0 && System.currentTimeMillis() < end) sleep(20);
                    for (int i = 0; i < 3; i++) {
                        Replica.Status s = c.nodes[i].status();
                        check(s.halted().isEmpty() && (i != 0 || s.digestsVerified() > 0), "node ", i, ": halted='", s.halted(), "' verified=", s.digestsVerified());
                    }
                }
            }},
            {"a_lagging_pipeline_replica_catches_up_from_a_snapshot_of_real_book_state", (T.Body) () -> {
                try (Cluster c = pipeCluster(3, cfg -> cfg.compactEvery = 40)) {
                    int l = c.waitLeader(5000), lag = l == 0 ? 1 : 0;
                    Random rng = new Random(2);
                    long[] next = {0};
                    Replica.ProposeResult[] last = new Replica.ProposeResult[1];
                    Runnable send10 = () -> { for (int i = 0; i < 10; i++) { last[0] = c.propose(randomBatch(rng, next, 15)); check(last[0].ok(), "batch: ", last[0]); } };
                    send10.run();
                    c.stop(lag);
                    for (int i = 0; i < 30; i++) send10.run();
                    long end = System.currentTimeMillis() + 10_000;
                    while (c.nodes[l].status().base() == 0 && System.currentTimeMillis() < end) sleep(20);
                    check(c.nodes[l].status().base() > 0, "leader never compacted");
                    c.restart(lag);
                    for (int i = 0; i < 5; i++) { last[0] = c.propose(randomBatch(rng, next, 15)); check(last[0].ok(), "batch"); }
                    allEqual(hashes(c, last[0].index(), 20_000), "state differs after snapshot install");
                    check(c.nodes[lag].status().base() > 0, "caught up without a snapshot install");
                }
            }},
            {"pipeline_state_survives_a_whole_cluster_restart_from_disk", (T.Body) () -> {
                try (Cluster c = pipeCluster(3, cfg -> { cfg.compactEvery = 30; cfg.fsync = true; })) {
                    c.waitLeader(5000);
                    Random rng = new Random(3);
                    long[] next = {0};
                    Replica.ProposeResult last = null;
                    for (int i = 0; i < 150; i++) { last = c.propose(randomBatch(rng, next, 10)); check(last.ok(), "batch: ", last); }
                    List<byte[]> before = hashes(c, last.index(), 10_000);
                    for (int i = 0; i < 3; i++) c.stop(i);
                    for (int i = 0; i < 3; i++) c.restart(i);
                    c.waitLeader(10_000);
                    // The leader's no-op advances the log by one; compare after it applies.
                    List<byte[]> after = hashes(c, last.index() + 1, 10_000);
                    for (int i = 0; i < after.size(); i++) check(Arrays.equals(after.get(i), before.get(0)), "replica ", i, " recovered different state");
                }
            }},
        });
    }
}
