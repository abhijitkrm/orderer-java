// End to end: real replicas over TCP with real files (mirrors orderer-rust's
// orderer-cluster/tests, orderer-cpp's cluster_e2e_test and orderer-go's cluster_test).
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.ClusterHarness.*;
import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.Consensus.Role;
import io.github.abhijitkrm.orderer.cluster.Client;
import io.github.abhijitkrm.orderer.cluster.ClientServer;
import io.github.abhijitkrm.orderer.cluster.Codec;
import io.github.abhijitkrm.orderer.cluster.RecorderSm;
import io.github.abhijitkrm.orderer.cluster.Replica;
import io.github.abhijitkrm.orderer.cluster.Replica.Kind;
import io.github.abhijitkrm.orderer.cluster.Transport.Member;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public final class ClusterTest {
    static void fill(Cluster c, long from, long to) { for (long i = from; i < to; i++) c.mustPropose(payload(i)); }

    static Command limit(long id) { return Command.newLimit(id, Side.Bid, 10000, 1, Tif.Gtc); }

    interface Scenario { void run(Cluster c) throws Exception; }

    static Object[] test(String name, int nodes, java.util.function.Consumer<Replica.Config> tune, boolean dedup, Scenario s) {
        return new Object[] {name, (T.Body) () -> { try (Cluster c = new Cluster(nodes, tune, dedup)) { s.run(c); } }};
    }

    public static void main(String[] args) {
        T.runAll(new Object[][] {
            test("elects_a_leader_and_replicates_in_order", 3, null, false, c -> {
                c.waitLeader(5000);
                fill(c, 0, 500);
                check(same(c.waitConverged(15_000), seq(0, 500)), "replicated log");
            }),
            test("a_single_node_cluster_works", 1, null, false, c -> {
                c.waitLeader(5000);
                fill(c, 0, 100);
                check(c.waitConverged(10_000).size() == 100, "100 entries");
            }),
            test("followers_refuse_proposals_and_point_to_the_leader", 3, null, false, c -> {
                int l = c.waitLeader(5000);
                c.mustPropose(payload(0));
                c.waitApplied(new int[] {0, 1, 2}, 1, 10_000);
                int f = l == 0 ? 1 : 0;
                Replica.ProposeResult r = c.nodes[f].propose(payload(1));
                check(r.kind() == Kind.NotLeader && r.hint() == l, "no redirect: ", r, " (leader ", l, ")");
            }),
            test("leader_crash_loses_no_acknowledged_command", 3, null, false, c -> {
                c.waitLeader(5000);
                Set<Long> acked = new HashSet<>(), maybe = new HashSet<>();
                for (long i = 0; i < 200; i++) (c.propose(payload(i)).ok() ? acked : maybe).add(i);
                int old = c.leader();
                c.crash(old);
                for (long i = 200; i < 600; i++) (c.propose(payload(i)).ok() ? acked : maybe).add(i);
                c.restart(old);
                Set<Long> seen = new HashSet<>();
                long prev = 0;
                for (byte[] p : c.waitConverged(20_000)) {
                    long id = idOf(p);
                    check(seen.add(id), "command ", id, " applied twice");
                    check(id >= prev, "commands reordered");
                    prev = id;
                }
                for (long a : acked) check(seen.contains(a), "acknowledged command ", a, " lost");
                for (long g : seen) check(acked.contains(g) || maybe.contains(g), g, " was never proposed");
            }),
            test("an_isolated_leader_cannot_commit_and_its_uncommitted_entries_vanish", 3, cfg -> cfg.proposeTimeoutMs = 700, false, c -> {
                int old = c.waitLeader(5000);
                fill(c, 0, 20);
                c.waitApplied(new int[] {0, 1, 2}, 20, 10_000);
                c.nodes[old].filter.isolate(old, List.of(0, 1, 2));
                Kind k = c.nodes[old].propose(payload(1000)).kind();
                check(k == Kind.Timeout || k == Kind.NotLeader || k == Kind.Superseded, "an isolated leader must not acknowledge: ", k);
                // The majority elects a new leader and commits.
                long end = System.currentTimeMillis() + 10_000;
                int newl = -1;
                while (newl < 0 && System.currentTimeMillis() < end) {
                    for (int i = 0; i < 3; i++) if (i != old && c.nodes[i].status().role() == Role.LEADER) newl = i;
                    sleep(20);
                }
                check(newl >= 0, "majority did not elect a leader");
                for (long i = 20; i < 60; i++) {
                    for (;;) {
                        Replica.ProposeResult p = c.nodes[newl].propose(payload(i));
                        if (p.ok()) break;
                        check(p.kind() == Kind.Busy || p.kind() == Kind.NotLeader, "majority side failed: ", p);
                        sleep(5);
                    }
                }
                c.nodes[old].filter.heal();
                check(same(c.waitConverged(15_000), seq(0, 60)), "entry 1000 must not survive");
            }),
            test("a_lagging_node_catches_up_by_snapshot", 3, cfg -> cfg.compactEvery = 40, false, c -> {
                int l = c.waitLeader(5000), lag = l == 0 ? 1 : 0;
                fill(c, 0, 10);
                c.waitApplied(new int[] {0, 1, 2}, 10, 10_000);
                c.stop(lag);
                fill(c, 10, 400);
                long end = System.currentTimeMillis() + 10_000;
                while (c.nodes[l].status().base() == 0 && System.currentTimeMillis() < end) sleep(20);
                check(c.nodes[l].status().base() > 0, "leader never compacted");
                c.restart(lag);
                check(same(c.waitConverged(20_000), seq(0, 400)), "after snapshot catch-up");
                check(c.nodes[lag].status().base() > 0, "caught up without a snapshot install");
            }),
            test("a_whole_cluster_restart_recovers_from_disk", 3, cfg -> { cfg.compactEvery = 50; cfg.fsync = true; }, false, c -> {
                c.waitLeader(5000);
                fill(c, 0, 300);
                c.waitConverged(10_000);
                for (int i = 0; i < 3; i++) c.stop(i);
                for (int i = 0; i < 3; i++) c.restart(i);
                c.waitLeader(10_000);
                fill(c, 300, 400);
                check(same(c.waitConverged(15_000), seq(0, 400)), "after full restart");
            }),
            test("a_torn_wal_tail_recovers_and_a_corrupt_wal_refuses_to_start", 3, cfg -> cfg.fsync = true, false, c -> {
                int l = c.waitLeader(5000), f = l == 0 ? 1 : 0;
                fill(c, 0, 100);
                c.waitConverged(10_000);
                c.crash(f);
                Path wal = c.dir.resolve("n" + f).resolve("wal").resolve("raft.wal");
                // Half a frame of garbage, as a torn write leaves.
                Files.write(wal, new byte[] {0x40, 0, 0, 0, (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 1, 2, 3}, StandardOpenOption.APPEND);
                c.restart(f);
                fill(c, 100, 150);
                check(same(c.waitConverged(10_000), seq(0, 150)), "after torn-tail recovery");
                // Now real damage: flip a byte early in the file, with valid frames after it.
                c.stop(f);
                byte[] data = Files.readAllBytes(wal);
                data[40] ^= (byte) 0xff;
                Files.write(wal, data);
                boolean refused = false;
                try { Replica.start(c.config(f), new RecorderSm(), null).crash(); } catch (IOException e) { refused = true; }
                check(refused, "a node with mid-log damage must not join");
            }),
            test("state_divergence_halts_the_odd_node_out", 3, cfg -> cfg.digestEvery = 16, false, c -> {
                c.stop(2);
                c.shared[2] = new Shared();
                c.shared[2].corruptAt = 5;
                c.start(2);
                c.waitLeader(5000);
                // If the corrupted node happens to lead, it halts itself mid-run and
                // proposals in flight to it have no definite answer; the majority carries on.
                int acked = 0;
                for (long i = 0; i < 200; i++) if (c.propose(payload(i)).ok()) acked++;
                check(acked >= 150, "only ", acked, " of 200 acknowledged");
                long end = System.currentTimeMillis() + 10_000;
                String why = "";
                while (System.currentTimeMillis() < end) {
                    why = c.nodes[2].status().halted();
                    if (!why.isEmpty()) break;
                    sleep(20);
                }
                check(why.contains("diverged"), "divergence went undetected: '", why, "'");
                check(c.nodes[0].status().halted().isEmpty() && c.nodes[1].status().halted().isEmpty(), "a healthy node halted");
            }),
            test("matching_state_passes_digest_checks", 3, cfg -> cfg.digestEvery = 16, false, c -> {
                c.waitLeader(5000);
                fill(c, 0, 200);
                c.waitConverged(10_000);
                long end = System.currentTimeMillis() + 5000;
                while (c.nodes[0].status().digestsVerified() == 0 && System.currentTimeMillis() < end) sleep(20);
                check(c.nodes[0].status().digestsVerified() > 0, "no digest was ever verified");
                for (int i = 0; i < 3; i++) check(c.nodes[i].status().halted().isEmpty(), "node ", i, " halted");
            }),
            test("hostile_connections_do_not_disturb_the_cluster", 3, null, false, c -> {
                c.waitLeader(5000);
                String target = c.members.get(0).addr();
                byte[][] junk = {
                    new byte[64],
                    {(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f, 0, 0, 0, 0, 1},
                    {10, 0, 0, 0, 0, 0, 0, 0, 6, 9, 1, 2, 3, 4, 5, 6, 7, 8},
                    {},
                };
                java.util.Arrays.fill(junk[0], (byte) 0xff);
                for (byte[] j : junk) {
                    int colon = target.lastIndexOf(':');
                    try (Socket s = new Socket(target.substring(0, colon), Integer.parseInt(target.substring(colon + 1)))) { s.getOutputStream().write(j); }
                    catch (IOException ignored) {}
                }
                fill(c, 0, 100);
                check(c.waitConverged(10_000).size() == 100, "100 entries");
            }),
            new Object[] {"five_nodes_survive_two_failures", (T.Body) () -> {
                try (Cluster c = new Cluster(5, null, false)) {
                    c.waitLeader(5000);
                    fill(c, 0, 100);
                    int l = c.leader(), other = 0;
                    c.crash(l);
                    while (other == l) other++;
                    c.crash(other);
                    c.waitLeader(15_000);
                    fill(c, 100, 200);
                    check(same(c.waitConverged(15_000), seq(0, 200)), "after two failures");
                }
            }},
            test("three_cannot_commit_with_two_down", 3, cfg -> cfg.proposeTimeoutMs = 500, false, c -> {
                int l = c.waitLeader(5000);
                c.mustPropose(payload(0));
                for (int i = 0; i < 3; i++) if (i != l) c.crash(i);
                Replica.ProposeResult r = c.nodes[l].propose(payload(1));
                check(r.kind() == Kind.Timeout, "no quorum, no ack: ", r);
                check(same(c.shared[l].payloads(), seq(0, 1)), "an unacknowledged entry was applied");
            }),
            test("a_slow_leader_state_machine_does_not_cost_it_leadership", 3, cfg -> {
                cfg.maxPending = 100000;
                cfg.proposeTimeoutMs = 30_000;
                cfg.consensus = new Consensus.Config(cfg.id == 0 ? 8 : 40, 2, 256, true);   // node 0 wins the first election
            }, false, c -> {
                check(c.waitLeader(5000) == 0, "node 0 should lead");
                // The slow machine is node 0's; each apply outlasts the followers' election timeout.
                c.shared[0].applyDelayMs.set(700);
                long term0 = c.nodes[0].status().term();
                List<CompletableFuture<Replica.ProposeResult>> fs = new ArrayList<>();
                for (long i = 0; i < 3; i++) fs.add(c.nodes[0].proposeAsync(payload(i)));
                for (var f : fs) check(f.join().ok(), "proposal");
                Replica.Status s = c.nodes[0].status();
                check(s.role() == Role.LEADER && s.term() == term0, "leadership was lost while the state machine was slow");
                c.waitApplied(new int[] {1, 2}, 3, 5000);
                c.waitApplied(new int[] {0}, 3, 30_000);
            }),
            test("a_cluster_with_one_shared_key_works_and_a_wrong_key_cannot_join", 3, cfg -> cfg.authKey = "right".getBytes(), false, c -> {
                int l = c.waitLeader(5000), f = l == 0 ? 1 : 0;
                fill(c, 0, 100);
                check(c.waitConverged(10_000).size() == 100, "100 entries");
                c.stop(f);
                Replica.Config cfg = c.config(f);
                cfg.authKey = "wrong".getBytes();
                Replica rogue = Replica.start(cfg, new RecorderSm(), null);
                try {
                    sleep(1500);
                    check(rogue.status().commit() == 0, "a node with the wrong key received log entries");
                    long term = c.nodes[l].status().term();
                    fill(c, 100, 140);
                    check(c.nodes[l].status().term() == term, "an outsider disturbed the cluster");
                } finally { rogue.crash(); }
            }),
            test("the_client_protocol_sessions_and_linearizable_reads", 3, null, true, c -> {
                int l = c.waitLeader(5000);
                List<String> addrs = freeAddrs(3);
                List<ClientServer> servers = new ArrayList<>();
                List<Member> pairs = new ArrayList<>();
                try {
                    for (int i = 0; i < 3; i++) { servers.add(new ClientServer(addrs.get(i), c.nodes[i], null)); pairs.add(new Member(i, addrs.get(i))); }
                    // Followers first: the client must find the leader by hint.
                    List<Member> order = new ArrayList<>();
                    for (int i = 0; i < 3; i++) if (i != l) order.add(pairs.get(i));
                    order.add(pairs.get(l));
                    try (Client client = new Client(order).withSession(7)) {
                        for (long i = 1; i <= 20; i++) {
                            Client.Result r = client.proposeSession(new long[] {1}, new Command[] {limit(i)});
                            check(r.ok(), "session batch ", i, ": ", r);
                        }
                    }
                    // The same session batch committed twice (a retry) takes effect once.
                    byte[] dup = Codec.encodeBatch(new long[] {1}, new Command[] {limit(999)}, new Codec.SessionKey(8, 1));
                    c.mustPropose(dup);
                    c.mustPropose(dup);
                    c.waitConverged(10_000);
                    int with999 = 0;
                    for (byte[] p : c.shared[0].payloads())
                        for (Command cm : Codec.decodeBatch(p).cmds()) if (cm instanceof Command.New n && n.orderId() == 999) with999++;
                    check(with999 == 1, "a retried batch took effect ", with999, " times");
                    // Status, hash and barrier over the wire; a follower cannot serve a barrier.
                    Client.Conn conn = Client.Conn.connect(addrs.get(l), null);
                    check(conn != null, "cannot connect");
                    if (conn != null) {
                        for (int i = 0; i < 3; i++) { String st = conn.status(); check(st != null && st.contains("role=Leader"), "status: ", st); }
                        check(conn.hash() != null, "hash");
                        check(conn.barrier() >= 0, "barrier");
                        conn.close();
                    }
                    Client.Conn fc = Client.Conn.connect(addrs.get(l == 0 ? 1 : 0), null);
                    check(fc != null && fc.barrier() < 0, "a follower served a barrier");
                    if (fc != null) fc.close();
                    // A client without a session cannot use proposeSession.
                    Client plain = new Client(pairs);
                    check(plain.proposeSession(new long[] {1}, new Command[] {limit(1)}).kind() == Client.Kind.Protocol, "plain client");
                } finally { for (ClientServer s : servers) s.shutdown(); }
            }),
            test("a_deposed_leader_cannot_serve_a_stale_read", 3, cfg -> cfg.proposeTimeoutMs = 600, false, c -> {
                int old = c.waitLeader(5000);
                c.mustPropose(payload(1));
                c.waitApplied(new int[] {0, 1, 2}, 1, 10_000);
                c.nodes[old].filter.isolate(old, List.of(0, 1, 2));
                long end = System.currentTimeMillis() + 10_000;
                int newl = -1;
                while (newl < 0 && System.currentTimeMillis() < end) {
                    for (int i = 0; i < 3; i++) if (i != old && c.nodes[i].barrier().ok()) newl = i;
                    sleep(20);
                }
                check(newl >= 0, "no new leader served a barrier");
                check(c.nodes[newl].propose(payload(2)).ok() && c.nodes[newl].barrier().ok(), "new leader cannot commit");
                check(c.shared[old].count() == 1, "the isolated node applied entries it could not have received");
                check(!c.nodes[old].barrier().ok(), "a deposed leader served a barrier");
            }),
        });
    }
}
