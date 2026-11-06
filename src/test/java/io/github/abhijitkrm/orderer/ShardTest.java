// Scale-out: the group assignment (checked against values computed
// independently from the splitmix64 definition, which the Rust, C++ and Go maps
// use too) and two replica groups end to end.
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.ClusterHarness.*;
import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.cluster.Client;
import io.github.abhijitkrm.orderer.cluster.ClientServer;
import io.github.abhijitkrm.orderer.cluster.Codec;
import io.github.abhijitkrm.orderer.cluster.ShardMap;
import io.github.abhijitkrm.orderer.cluster.ShardedClient;
import io.github.abhijitkrm.orderer.cluster.Transport.Member;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ShardTest {
    /// One replicated group with client ports.
    static final class Group implements AutoCloseable {
        final Cluster c;
        final List<Member> clients = new ArrayList<>();
        final ClientServer[] servers = new ClientServer[3];

        Group(long clusterId) throws Exception {
            c = new Cluster(3, cfg -> cfg.clusterId = clusterId, true);
            for (String a : freeAddrs(3)) clients.add(new Member(clients.size(), a));
            for (int i = 0; i < 3; i++) servers[i] = new ClientServer(clients.get(i).addr(), c.nodes[i], null);
            c.waitLeader(5000);
        }

        /// Bring node i back, with its client port bound to the new replica.
        void restartNode(int i) throws Exception {
            servers[i].shutdown();
            c.restart(i);
            servers[i] = new ClientServer(clients.get(i).addr(), c.nodes[i], null);
        }

        Client client(long session) {
            Client cl = new Client(clients).withSession(session);
            cl.retryBudgetMs = 3000;
            return cl;
        }

        @Override public void close() {
            for (ClientServer s : servers) if (s != null) s.shutdown();
            c.close();
        }
    }

    static Command limit(long id) { return Command.newLimit(id, Side.Bid, 10000, 1, Tif.Gtc); }

    static byte[] symbolBatch(long[] next, long[] syms, int n) {
        long[] ss = new long[n];
        Command[] cs = new Command[n];
        for (int i = 0; i < n; i++) { ss[i] = syms[i % syms.length]; cs[i] = limit(++next[0]); }
        return Codec.encodeBatch(ss, cs, null);
    }

    public static void main(String[] args) {
        T.runAll(new Object[][] {
            {"group_assignment_matches_the_other_languages", (T.Body) () -> {
                long[] syms = {0, 1, 2, 3, 7, 100, 4095, 65535, 4294967295L};
                int[] g4 = {3, 1, 2, 1, 3, 0, 3, 2, 0}, g3 = {1, 2, 1, 0, 0, 2, 2, 1, 2};
                ShardMap m4 = new ShardMap(4), m3 = new ShardMap(3);
                for (int i = 0; i < syms.length; i++) {
                    check(m4.groupOf(syms[i]) == g4[i], "groups=4 sym=", syms[i], ": ", m4.groupOf(syms[i]), " want ", g4[i]);
                    check(m3.groupOf(syms[i]) == g3[i], "groups=3 sym=", syms[i], ": ", m3.groupOf(syms[i]), " want ", g3[i]);
                }
            }},
            {"every_symbol_has_one_stable_group_and_groups_are_balanced", (T.Body) () -> {
                ShardMap m = new ShardMap(4);
                int[] counts = new int[4];
                for (long s = 0; s < 40_000; s++) { int g = m.groupOf(s); check(g == m.groupOf(s), "unstable"); counts[g]++; }
                for (int c : counts) check(c >= 9000 && c <= 11000, "unbalanced: ", java.util.Arrays.toString(counts));
            }},
            {"a_groups_symbols_still_spread_over_its_partitions", (T.Body) () -> {
                // If the group hash correlated with the pipeline's partition hash, a group's symbols would pile onto one partition.
                ShardMap m = new ShardMap(2);
                int[][] grid = new int[2][2];
                for (long s = 0; s < 20_000; s++) grid[m.groupOf(s)][Routing.hashPartition(s, 2)]++;
                for (int[] row : grid) for (int c : row) check(c >= 4000 && c <= 6000, "skewed: ", java.util.Arrays.deepToString(grid));
            }},
            {"split_keeps_each_symbols_order_and_loses_nothing", (T.Body) () -> {
                ShardMap m = new ShardMap(3);
                long[] syms = new long[300];
                Command[] cmds = new Command[300];
                for (int i = 0; i < 300; i++) { syms[i] = i % 17; cmds[i] = limit(i + 1); }
                int total = 0;
                ShardMap.Part[] parts = m.split(syms, cmds);
                for (int g = 0; g < parts.length; g++) {
                    total += parts[g].cmds().size();
                    long prev = 0;
                    for (int i = 0; i < parts[g].cmds().size(); i++) {
                        check(m.groupOf(parts[g].syms().get(i)) == g, "command in the wrong group");
                        long id = ((Command.New) parts[g].cmds().get(i)).orderId();
                        check(id > prev, "order within a group changed");
                        prev = id;
                    }
                }
                check(total == 300, "split lost commands: ", total);
            }},
            {"groups_hold_only_their_symbols_in_per_symbol_order", (T.Body) () -> {
                try (Group g0 = new Group(11); Group g1 = new Group(12);
                     ShardedClient sc = new ShardedClient(List.of(g0.client(1), g1.client(1)))) {
                    long[] next = {0};
                    Map<Long, List<Long>> sent = new HashMap<>();
                    for (int b = 0; b < 40; b++) {
                        long[] syms = new long[25];
                        Command[] cmds = new Command[25];
                        for (int i = 0; i < 25; i++) {
                            syms[i] = (b * 25 + i) % 12;
                            cmds[i] = limit(++next[0]);
                            sent.computeIfAbsent(syms[i], k -> new ArrayList<>()).add(next[0]);
                        }
                        ShardedClient.Outcome[] out = sc.propose(syms, cmds);
                        for (int gi = 0; gi < out.length; gi++) check(!out[gi].sent() || out[gi].result().ok(), "group ", gi, ": ", out[gi].result());
                    }
                    Group[] gs = {g0, g1};
                    for (int gi = 0; gi < 2; gi++) {
                        gs[gi].c.waitConverged(10_000);
                        Map<Long, List<Long>> got = new HashMap<>();
                        for (byte[] p : gs[gi].c.shared[0].payloads()) {
                            Codec.Batch b = Codec.decodeBatch(p);
                            for (int i = 0; i < b.size(); i++) {
                                check(sc.map().groupOf(b.syms()[i]) == gi, "group ", gi, " holds symbol ", b.syms()[i], " of another group");
                                got.computeIfAbsent(b.syms()[i], k -> new ArrayList<>()).add(((Command.New) b.cmds()[i]).orderId());
                            }
                        }
                        for (var e : got.entrySet()) check(e.getValue().equals(sent.get(e.getKey())), "group ", gi, " symbol ", e.getKey(), ": order or content differs");
                    }
                }
            }},
            {"an_outage_in_one_group_does_not_stop_the_other", (T.Body) () -> {
                try (Group g0 = new Group(21); Group g1 = new Group(22)) {
                    ShardMap m = new ShardMap(2);
                    List<List<Long>> mine = List.of(new ArrayList<>(), new ArrayList<>());
                    for (long s = 0; mine.get(0).size() < 4 || mine.get(1).size() < 4; s++) mine.get(m.groupOf(s)).add(s);
                    // Take group 1 down entirely.
                    for (int i = 0; i < 3; i++) g1.c.crash(i);
                    long[] next = {0};
                    try (Client c0 = g0.client(1)) {
                        for (int i = 0; i < 50; i++) {
                            Client.Result r = c0.propose(symbolBatch(next, mine.get(0).stream().mapToLong(Long::longValue).limit(4).toArray(), 10));
                            check(r.ok(), "healthy group failed while its sibling was down: ", r);
                        }
                    }
                    // Group 1 comes back, and its data is intact and usable.
                    for (int i = 0; i < 3; i++) g1.restartNode(i);
                    g1.c.waitLeader(10_000);
                    try (Client c1 = g1.client(2)) {
                        Client.Result r = c1.propose(symbolBatch(next, mine.get(1).stream().mapToLong(Long::longValue).limit(4).toArray(), 10));
                        check(r.ok(), "restarted group: ", r);
                    }
                }
            }},
        });
    }
}
