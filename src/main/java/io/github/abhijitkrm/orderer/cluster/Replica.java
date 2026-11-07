package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.Consensus;
import io.github.abhijitkrm.orderer.Consensus.Effect;
import io.github.abhijitkrm.orderer.Consensus.Entry;
import io.github.abhijitkrm.orderer.Consensus.Persist;
import io.github.abhijitkrm.orderer.Consensus.Role;
import io.github.abhijitkrm.orderer.cluster.Transport.Inbound;
import io.github.abhijitkrm.orderer.cluster.Transport.Member;
import io.github.abhijitkrm.orderer.cluster.Wal.WalException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/// The replica runtime: one driver thread owns the protocol Node and executes
/// its effects (WAL writes on an I/O thread, network sends, state machine
/// applies on an applier thread, snapshots), as in orderer-rust (replica.rs;
/// spec/REPLICATION.md 13). Acknowledgement is at commit; the state machine
/// applies on its own thread so a slow one cannot starve heartbeats, and
/// producers are pushed back with Busy when it falls behind.
public final class Replica {
    /// Configuration of one replica.
    public static final class Config {
        public int id;
        public List<Member> members;
        public long clusterId = 1;
        public Path dir;
        public int tickMs = 10;
        public Consensus.Config consensus = new Consensus.Config(30, 3, 256, true);
        /// Snapshot and compact the log every N applied entries; 0 = never.
        public long compactEvery;
        /// Exchange state digests every N entries to detect divergence; 0 = off.
        public long digestEvery;
        /// false skips fsync: tests and benchmarks only, it loses durability.
        public boolean fsync = true;
        public int proposeTimeoutMs = 5000;
        public int maxPending = 4096;
        /// How far the state machine may lag the log before proposals get Busy.
        public long maxApplyBacklog = 8192;
        /// Pre-shared cluster key; null = no authentication.
        public byte[] authKey;

        public Config(int id, List<Member> members, Path dir) { this.id = id; this.members = members; this.dir = dir; }
        public Config copy() {
            Config c = new Config(id, members, dir);
            c.clusterId = clusterId; c.tickMs = tickMs; c.consensus = consensus; c.compactEvery = compactEvery;
            c.digestEvery = digestEvery; c.fsync = fsync; c.proposeTimeoutMs = proposeTimeoutMs; c.maxPending = maxPending;
            c.maxApplyBacklog = maxApplyBacklog; c.authKey = authKey;
            return c;
        }
    }

    public enum Kind {
        /// Committed on a quorum.
        Ok,
        /// Not the leader: try hint, if any.
        NotLeader,
        /// Back-pressure; retry shortly.
        Busy,
        /// No outcome in time; it may still commit.
        Timeout,
        /// Another leader's entry took the slot; the command is not in the log.
        Superseded,
        /// The node stopped or lost leadership first; it may still commit.
        Unknown,
        /// This replica is not running.
        Stopped
    }

    /// A proposal's outcome. hint is the leader this node last heard from, or -1.
    public record ProposeResult(Kind kind, long index, int hint) {
        public boolean ok() { return kind == Kind.Ok; }
        static ProposeResult of(Kind k) { return new ProposeResult(k, 0, -1); }
    }

    /// A replica's published state.
    public record Status(int id, Role role, long term, int leader, long commit, long applied, long base, long lastIndex,
                         int pending, String halted, long digestsVerified) {}

    /// (applied index, state digest).
    public record Hash(long index, byte[] digest) {}

    private enum Ev { INBOUND, PERSIST_DONE, PROPOSE, HASH, SNAPSHOT_STORED, DIGEST_COMPUTED, FATAL, STOP }

    private static final class Event {
        final Ev kind;
        Inbound in;
        long token, index;
        byte[] payload, digest;
        CompletableFuture<ProposeResult> reply;
        CompletableFuture<Hash> hashReply;
        String text;
        Event(Ev k) { kind = k; }
    }

    private enum Op { APPLY, SNAPSHOT, RESTORE, HASH, STOP }

    private static final class Job {
        final Op op;
        long index, term;
        Entry entry;
        CompletableFuture<Hash> hashReply;
        Job(Op o) { op = o; }
    }

    private record Pending(long term, CompletableFuture<ProposeResult> reply, long deadlineNanos) {}

    private record WalItem(Persist p) {}   // p == null: stop

    private final Config cfg;
    private final StateMachine sm;
    public final Filter filter;
    private final Wal wal;
    private final SnapStore snaps;
    private final Consensus.Node node;
    private final Transport transport;

    private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<WalItem> walQ = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Job> jobs = new LinkedBlockingQueue<>();
    private final Thread driver, walThread, applier;
    private final AtomicBoolean stopped = new AtomicBoolean(), abort = new AtomicBoolean(), done = new AtomicBoolean();
    private final AtomicLong smApplied = new AtomicLong();

    // Driver-thread state.
    private final Map<Long, Pending> pending = new HashMap<>();
    private final Map<Long, Long> compactTokens = new TreeMap<>();
    private final TreeMap<Long, Map<Integer, byte[]>> digests = new TreeMap<>();
    private long digestsVerified;
    private String halted = "";
    private boolean snapshotPending;

    private volatile Status status;

    public static Replica start(Config cfg, StateMachine sm, Filter filter) throws IOException {
        return new Replica(cfg, sm, filter == null ? new Filter() : filter);
    }

    public Config config() { return cfg; }

    private Replica(Config cfg, StateMachine sm, Filter filter) throws IOException {
        this.cfg = cfg; this.sm = sm; this.filter = filter;
        Wal.Opened o = Wal.open(cfg.dir.resolve("wal"));
        wal = o.wal();
        Consensus.Disk disk = o.disk();
        try {
            snaps = new SnapStore(cfg.dir.resolve("snap"));
            if (disk.base > 0) {
                SnapStore.Loaded s = snaps.get(disk.base);
                if (s == null) throw new IOException("log base " + disk.base + " needs a snapshot that cannot be read");
                try { sm.restore(disk.base, s.data()); }
                catch (StateMachine.RestoreException e) { throw new IOException("state machine: " + e.getMessage()); }
            }
            smApplied.set(disk.base);
            int[] peers = cfg.members.stream().mapToInt(Member::id).toArray();
            long seed = System.nanoTime() ^ cfg.clusterId ^ ((long) cfg.id << 32);
            node = new Consensus.Node(cfg.id, peers, cfg.consensus, seed, disk);
            status = new Status(cfg.id, Role.FOLLOWER, node.term(), -1, node.commitIndex(), node.appliedIndex(), node.base(), node.lastIndex(), 0, "", 0);
            transport = new Transport(cfg.id, cfg.members, cfg.clusterId, in -> { Event e = new Event(Ev.INBOUND); e.in = in; events.add(e); }, snaps, filter, cfg.authKey);
        } catch (IOException | RuntimeException e) { wal.close(); throw e; }
        walThread = new Thread(this::walLoop, "replica-wal-" + cfg.id);
        applier = new Thread(this::applierLoop, "replica-apply-" + cfg.id);
        driver = new Thread(this::run, "replica-driver-" + cfg.id);
        for (Thread t : List.of(walThread, applier, driver)) { t.setDaemon(true); t.start(); }
    }

    // ---- client API ----

    /// Submit a pre-encoded log entry payload and block until it is committed.
    public ProposeResult propose(byte[] payload) { return await(proposeAsync(payload)); }

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        catch (ExecutionException e) { throw new IllegalStateException(e.getCause()); }
    }

    /// Submit a payload; the result arrives on the returned future.
    public CompletableFuture<ProposeResult> proposeAsync(byte[] payload) {
        CompletableFuture<ProposeResult> reply = new CompletableFuture<>();
        if (stopped.get() || done.get()) { reply.complete(ProposeResult.of(Kind.Stopped)); return reply; }
        Event e = new Event(Ev.PROPOSE);
        e.payload = payload; e.reply = reply;
        events.add(e);
        if (done.get()) answerStragglers();   // the driver ended while we queued
        return reply;
    }

    /// A linearizable read barrier: commits an empty entry and waits for the
    /// state machine to apply it. A deposed leader cannot commit, so it cannot answer.
    public ProposeResult barrier() {
        ProposeResult r = propose(new byte[0]);
        if (!r.ok()) return r;
        long end = System.nanoTime() + 30_000_000_000L;
        while (smApplied.get() < r.index()) {
            if (System.nanoTime() > end) return ProposeResult.of(Kind.Timeout);
            try { Thread.sleep(0, 200_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return r;
    }

    public Status status() { return status; }

    /// (applied index, state digest), or null if the state machine does not support digests.
    public Hash stateHash() {
        if (stopped.get() || done.get()) return null;
        CompletableFuture<Hash> reply = new CompletableFuture<>();
        Event e = new Event(Ev.HASH);
        e.hashReply = reply;
        events.add(e);
        if (done.get()) answerStragglers();
        return await(reply);
    }

    /// Graceful stop: queued WAL writes are flushed first.
    public void stop() { shutdown(false); }

    /// Simulated power cut: threads stop at once; writes not yet fsynced are discarded.
    public void crash() { shutdown(true); }

    private void shutdown(boolean hard) {
        if (!stopped.getAndSet(true)) {
            if (hard) abort.set(true);
            events.add(new Event(Ev.STOP));
        }
        try { driver.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ---- WAL thread ----

    private void walLoop() {
        try {
            for (;;) {
                WalItem first = walQ.take();
                if (first.p() == null || abort.get()) return;
                List<Persist> batch = new ArrayList<>();
                batch.add(first.p());
                boolean quit = false;
                for (WalItem more; (more = walQ.poll()) != null; ) {
                    if (more.p() == null) { quit = true; break; }
                    batch.add(more.p());
                }
                boolean compacted = false;
                try {
                    for (Persist p : batch) { wal.append(p); compacted |= p.hasCompact; }
                    wal.sync(cfg.fsync);
                    if (abort.get()) return;
                    if (compacted) wal.rewrite();
                } catch (WalException e) { fatal("wal write failed: " + e.getMessage()); return; }
                Event d = new Event(Ev.PERSIST_DONE);
                d.token = batch.get(batch.size() - 1).token;
                events.add(d);
                if (quit) return;
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        finally { wal.close(); }
    }

    // ---- applier thread ----

    private void applierLoop() {
        try {
            for (;;) {
                Job j = jobs.take();
                try {
                    switch (j.op) {
                        case APPLY -> {
                            sm.apply(j.index, j.entry);
                            smApplied.set(j.index);
                            if (cfg.digestEvery > 0 && j.index % cfg.digestEvery == 0) {
                                byte[] d = sm.digest(j.index);
                                if (d != null) { Event e = new Event(Ev.DIGEST_COMPUTED); e.index = j.index; e.digest = d; events.add(e); }
                            }
                        }
                        case SNAPSHOT -> {
                            byte[] data = sm.snapshot(j.index);
                            snaps.put(j.index, j.term, data);
                            Event e = new Event(Ev.SNAPSHOT_STORED);
                            e.index = j.index;
                            events.add(e);
                        }
                        case RESTORE -> {
                            SnapStore.Loaded s = snaps.get(j.index);
                            if (s == null) { fatal("reading snapshot " + j.index); break; }
                            sm.restore(j.index, s.data());
                            smApplied.set(j.index);
                        }
                        case HASH -> {
                            long at = smApplied.get();
                            byte[] d = sm.deepDigest(at);
                            j.hashReply.complete(d == null ? null : new Hash(at, d));
                        }
                        case STOP -> { return; }
                    }
                } catch (Throwable e) {
                    // The state machine or its storage failed: stop applying, so nothing runs past the failure.
                    fatal("state machine " + j.op.name().toLowerCase() + ": " + e);
                    return;
                }
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /// Anything still queued that expects an answer gets one (the driver is gone).
    private void answerStragglers() {
        for (Event e; (e = events.poll()) != null; ) {
            if (e.reply != null) e.reply.complete(ProposeResult.of(Kind.Stopped));
            if (e.hashReply != null) e.hashReply.complete(null);
        }
    }

    private void fatal(String why) {
        Event e = new Event(Ev.FATAL);
        e.text = why;
        events.add(e);
    }

    // ---- driver ----

    private void run() {
        long tickNanos = cfg.tickMs * 1_000_000L;
        long nextTick = System.nanoTime() + tickNanos;
        boolean running = true;
        boolean[] replicate = new boolean[1];
        while (running) {
            replicate[0] = false;
            Event ev = null;
            try { ev = events.poll(Math.max(nextTick - System.nanoTime(), 0), TimeUnit.NANOSECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            if (ev != null) {
                running = handle(ev, replicate);
                for (int i = 0; running && i < 4096; i++) {
                    Event more = events.poll();
                    if (more == null) break;
                    running = handle(more, replicate);
                }
            }
            long now = System.nanoTime();
            if (now >= nextTick) {
                node.tick();
                nextTick = now + tickNanos;
                expire(now);
            }
            if (replicate[0]) node.replicate();
            pump();
            maybeCompact();
            pump();
            publishStatus();
            if (!halted.isEmpty()) break;
        }
        publishStatus();
        for (Pending p : pending.values()) p.reply().complete(ProposeResult.of(Kind.Unknown));
        pending.clear();
        done.set(true);
        answerStragglers();
        transport.shutdown();
        jobs.add(new Job(Op.STOP));
        join(applier);
        walQ.add(new WalItem(null));
        join(walThread);
    }

    private static void join(Thread t) {
        try { t.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private boolean handle(Event ev, boolean[] replicate) {
        switch (ev.kind) {
            case STOP -> { return false; }
            case PERSIST_DONE -> {
                node.persisted(ev.token);
                var it = compactTokens.entrySet().iterator();
                while (it.hasNext()) {
                    var e = it.next();
                    if (e.getKey() <= ev.token) { snaps.gc(e.getValue()); it.remove(); }
                }
            }
            case INBOUND -> {
                Inbound in = ev.in;
                switch (in.kind) {
                    case Msg -> node.step(in.from, in.msg);
                    case Snapshot -> {
                        System.err.printf("orderer-cluster node %d: installing snapshot %d from node %d%n", cfg.id, in.index, in.from);
                        node.installSnapshot(in.from, in.term, in.index, in.lastTerm);
                    }
                    case Digest -> { digests.computeIfAbsent(in.index, k -> new HashMap<>()).put(in.from, in.digest); checkDigests(in.index); }
                }
            }
            case PROPOSE -> {
                long behind = Math.max(node.appliedIndex() - smApplied.get(), 0);
                long idx;
                if (pending.size() >= cfg.maxPending || behind > cfg.maxApplyBacklog) {
                    ev.reply.complete(ProposeResult.of(Kind.Busy));
                } else if ((idx = node.propose(ev.payload)) >= 0) {
                    pending.put(idx, new Pending(node.term(), ev.reply, System.nanoTime() + cfg.proposeTimeoutMs * 1_000_000L));
                    replicate[0] = true;
                } else {
                    ev.reply.complete(new ProposeResult(Kind.NotLeader, 0, node.leader()));
                }
            }
            case HASH -> { Job j = new Job(Op.HASH); j.hashReply = ev.hashReply; jobs.add(j); }
            case SNAPSHOT_STORED -> {
                snapshotPending = false;
                if (ev.index > node.base() && ev.index <= node.appliedIndex()) node.compact(ev.index);
            }
            case DIGEST_COMPUTED -> {
                digests.computeIfAbsent(ev.index, k -> new HashMap<>()).put(cfg.id, ev.digest);
                for (Member m : cfg.members) if (m.id() != cfg.id) transport.sendDigest(m.id(), ev.index, ev.digest);
                checkDigests(ev.index);
            }
            case FATAL -> halt(ev.text);
        }
        return true;
    }

    private void expire(long now) {
        pending.values().removeIf(p -> {
            if (p.deadlineNanos() > now) return false;
            p.reply().complete(ProposeResult.of(Kind.Timeout));
            return true;
        });
    }

    private void pump() {
        for (;;) {
            List<Effect> effects = node.takeEffects();
            if (effects.isEmpty()) return;
            for (Effect e : effects) {
                switch (e.kind) {
                    case SEND -> transport.send(e.to, e.msg);
                    case PERSIST -> {
                        if (e.persist.hasCompact) compactTokens.put(e.persist.token, e.persist.compactIndex);
                        walQ.add(new WalItem(e.persist));
                    }
                    case APPLY -> committed(e.index, e.entry);
                    case NEED_SNAPSHOT -> transport.sendSnapshot(e.to, node.term(), e.index, e.term);
                    case RESTORE -> { Job j = new Job(Op.RESTORE); j.index = e.index; jobs.add(j); }
                }
            }
        }
    }

    /// An entry committed: acknowledge its proposer, then queue it for the state machine.
    private void committed(long index, Entry entry) {
        Pending p = pending.remove(index);
        if (p != null) p.reply().complete(p.term() == entry.term() ? new ProposeResult(Kind.Ok, index, -1) : ProposeResult.of(Kind.Superseded));
        Job j = new Job(Op.APPLY);
        j.index = index; j.entry = entry;
        jobs.add(j);
    }

    /// Fail-stop if this node's state differs from the majority's.
    private void checkDigests(long index) {
        Map<Integer, byte[]> seen = digests.get(index);
        if (seen == null || !seen.containsKey(cfg.id)) return;
        String own = HexFormat.of().formatHex(seen.get(cfg.id));
        int n = cfg.members.size(), quorum = n / 2 + 1;
        Map<String, Integer> groups = new HashMap<>();
        for (byte[] d : seen.values()) groups.merge(HexFormat.of().formatHex(d), 1, Integer::sum);
        boolean otherMajority = groups.entrySet().stream().anyMatch(g -> g.getValue() >= quorum && !g.getKey().equals(own));
        if (groups.get(own) >= quorum) {
            if (seen.size() == n) { digestsVerified++; digests.remove(index); }
        } else if (otherMajority) {
            halt("state diverged from the majority at index " + index + " (own digest " + own + ")");
        } else if (seen.size() == n) {
            halt("state digests at index " + index + " have no majority");
        }
        if (cfg.digestEvery > 0) {
            long span = cfg.digestEvery * 64;
            digests.headMap(index > span ? index - span : 0).clear();
        }
    }

    private void halt(String why) {
        if (!halted.isEmpty()) return;
        System.err.printf("orderer-cluster node %d: HALT: %s%n", cfg.id, why);
        halted = why;
    }

    private void maybeCompact() {
        if (cfg.compactEvery == 0 || !halted.isEmpty() || snapshotPending) return;
        long applied = node.appliedIndex();
        if (applied < node.base() || applied - node.base() < cfg.compactEvery) return;
        Entry e = node.entryAt(applied);
        if (e == null) return;
        // The applier snapshots after applying `applied`, then reports back;
        // only then does the log drop the prefix.
        snapshotPending = true;
        Job j = new Job(Op.SNAPSHOT);
        j.index = applied; j.term = e.term();
        jobs.add(j);
    }

    private void publishStatus() {
        status = new Status(cfg.id, node.role(), node.term(), node.leader(), node.commitIndex(), smApplied.get(), node.base(),
                node.lastIndex(), pending.size(), halted, digestsVerified);
    }
}
