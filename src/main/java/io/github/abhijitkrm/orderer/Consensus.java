// Consensus — the replication protocol core (spec/REPLICATION.md) as a pure
// state machine: no I/O, no clock. The caller feeds inputs (tick, step,
// propose, persisted, compact, installSnapshot) and executes the effects it
// emits. A faithful port of orderer-rust's orderer-consensus; the conformance
// vectors in vectors/replication/ prove the two produce identical effects and
// state for the same inputs.
package io.github.abhijitkrm.orderer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Consensus {
    private Consensus() {}

    /** "No node" in optional node fields (leader, vote). */
    public static final int NONE = -1;

    /** xorshift64* seeded through splitmix64, bit-identical to the Rust port. */
    public static final class Rng {
        private long s;
        public Rng(long seed) {
            long z = seed + 0x9E3779B97F4A7C15L;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            z ^= z >>> 31;
            s = z == 0 ? 1 : z;
        }
        long next() {
            long x = s;
            x ^= x >>> 12;
            x ^= x << 25;
            x ^= x >>> 27;
            s = x;
            return x * 0x2545F4914F6CDD1DL;
        }
        long below(long n) { return Long.remainderUnsigned(next(), n); }
    }

    public record Entry(long term, byte[] data) {}

    public enum MsgKind { VOTE, VOTE_REPLY, APPEND, APPEND_REPLY }

    /** One struct for all four message kinds; {@code kind} says which fields apply. */
    public static final class Msg {
        public MsgKind kind;
        public long term;
        public long lastIndex, lastTerm;          // Vote
        public boolean pre;                       // Vote, VoteReply
        public boolean granted;                   // VoteReply
        public long prevIndex, prevTerm, commit;  // Append
        public List<Entry> entries = List.of();   // Append
        public boolean success;                   // AppendReply
        public long index;                        // AppendReply

        public static Msg vote(long term, long lastIndex, long lastTerm, boolean pre) {
            Msg m = new Msg(); m.kind = MsgKind.VOTE; m.term = term; m.lastIndex = lastIndex; m.lastTerm = lastTerm; m.pre = pre; return m;
        }
        public static Msg voteReply(long term, boolean granted, boolean pre) {
            Msg m = new Msg(); m.kind = MsgKind.VOTE_REPLY; m.term = term; m.granted = granted; m.pre = pre; return m;
        }
        public static Msg append(long term, long prevIndex, long prevTerm, List<Entry> es, long commit) {
            Msg m = new Msg(); m.kind = MsgKind.APPEND; m.term = term; m.prevIndex = prevIndex; m.prevTerm = prevTerm; m.entries = es; m.commit = commit; return m;
        }
        public static Msg appendReply(long term, boolean success, long index) {
            Msg m = new Msg(); m.kind = MsgKind.APPEND_REPLY; m.term = term; m.success = success; m.index = index; return m;
        }
    }

    /** One atomic disk write: hard state, optional log-prefix discard, truncate to from-1, append. */
    public static final class Persist {
        public long token, term, from;
        public int votedFor = NONE;
        public List<Entry> entries = List.of();
        public boolean hasCompact;
        public long compactIndex, compactTerm;
    }

    public enum EffectKind { SEND, PERSIST, APPLY, NEED_SNAPSHOT, RESTORE }

    public static final class Effect {
        public EffectKind kind;
        public int to;           // Send, NeedSnapshot
        public Msg msg;          // Send
        public Persist persist;  // Persist
        public long index;       // Apply, NeedSnapshot, Restore
        public Entry entry;      // Apply
        public long term;        // NeedSnapshot, Restore
    }

    /** What survives a crash. */
    public static final class Disk {
        public long term, base, baseTerm;
        public int votedFor = NONE;
        public List<Entry> log = List.of();
    }

    public record Config(long electionTicks, long heartbeatTicks, int maxBatch, boolean preVote) {}

    public enum Role {
        FOLLOWER("Follower"), PRE_CANDIDATE("PreCandidate"), CANDIDATE("Candidate"), LEADER("Leader");
        public final String label;
        Role(String l) { label = l; }
    }

    private static final class Write {
        final long token, upto, epoch;
        final List<Effect> gated = new ArrayList<>();
        Write(long token, long upto, long epoch) { this.token = token; this.upto = upto; this.epoch = epoch; }
    }

    /** One replica's protocol state. */
    public static final class Node {
        private final int id;
        private final int[] peers;
        private final Config cfg;
        private final Rng rng;
        private long term;
        private int votedFor;
        private List<Entry> log;
        private long base, baseTerm;
        private Role role = Role.FOLLOWER;
        private int leader = NONE;
        private long commit, applied, durable, epoch, nextTok = 1;
        private final ArrayDeque<Write> writes = new ArrayDeque<>();
        private final boolean[] votes, heard;
        private final long[] nextIndex, matchIndex, snapWait;
        private long electionElapsed, electionTimeout, heartbeatElapsed;
        private List<Effect> out = new ArrayList<>();
        private String fault = "";

        public Node(int id, int[] peerIds, Config cfg, long seed, Disk disk) {
            this.id = id;
            this.peers = peerIds.clone();
            Arrays.sort(this.peers);
            this.cfg = cfg;
            this.rng = new Rng(seed ^ ((long) id << 56));
            this.term = disk.term;
            this.votedFor = disk.votedFor;
            this.log = new ArrayList<>(disk.log);
            this.base = disk.base;
            this.baseTerm = disk.baseTerm;
            this.commit = this.applied = disk.base;
            this.durable = disk.base + disk.log.size();
            int n = peers.length;
            votes = new boolean[n];
            heard = new boolean[n];
            nextIndex = new long[n];
            matchIndex = new long[n];
            snapWait = new long[n];
            Arrays.fill(nextIndex, 1);
            resetTimeout();
        }

        // ---- observers ----
        public int id() { return id; }
        public Role role() { return role; }
        public long term() { return term; }
        public int leader() { return leader; }
        public long commitIndex() { return commit; }
        public long appliedIndex() { return applied; }
        public long base() { return base; }
        public long lastIndex() { return base + log.size(); }
        public String fault() { return fault; }

        public List<Effect> takeEffects() {
            List<Effect> o = out;
            out = new ArrayList<>();
            return o;
        }

        /** The log entry at index i, or null if the node no longer holds it. */
        public Entry entryAt(long i) {
            return i > base && i - base - 1 < log.size() ? log.get((int) (i - base - 1)) : null;
        }

        // ---- inputs ----
        public void tick() {
            for (int i = 0; i < snapWait.length; i++) if (snapWait[i] > 0) snapWait[i]--;
            if (role == Role.LEADER) {
                heartbeatElapsed++;
                if (heartbeatElapsed >= cfg.heartbeatTicks()) {
                    heartbeatElapsed = 0;
                    broadcastAppend();
                }
                electionElapsed++;
                if (electionElapsed >= electionTimeout) {
                    // Check-quorum: step down without contact from a majority.
                    int me = pos(id), got = 0;
                    for (int i = 0; i < heard.length; i++) if (heard[i] || i == me) got++;
                    if (got < quorum()) { role = Role.FOLLOWER; leader = NONE; }
                    Arrays.fill(heard, false);
                    resetTimeout();
                }
                return;
            }
            electionElapsed++;
            if (electionElapsed >= electionTimeout) startElection();
        }

        /** Appends client data; only the leader accepts. Returns the entry's index, or -1. */
        public long propose(byte[] data) {
            if (role != Role.LEADER) return -1;
            leaderAppend(new Entry(term, data));
            return lastIndex();
        }

        public void replicate() { if (role == Role.LEADER) broadcastAppend(); }

        /** The host stored a snapshot after applying {@code index}; drop the log prefix it covers. */
        public void compact(long index) {
            if (index <= base || index > applied) { fault = "compact outside (base, applied]"; return; }
            long t = termAt(index);
            log = new ArrayList<>(log.subList((int) (index - base), log.size()));
            base = index;
            baseTerm = t;
            persistWith(lastIndex() + 1, List.of(), true, index, t);
        }

        /** The host received and stored the leader's snapshot. */
        public void installSnapshot(int from, long termIn, long index, long lastTerm) {
            if (termIn < term) return;
            becomeFollower(termIn, from);
            long replyTerm = term;
            if (index <= commit) {
                sendGated(from, Msg.appendReply(replyTerm, true, commit));
                return;
            }
            if (index <= lastIndex() && termAt(index) == lastTerm) {
                log = new ArrayList<>(log.subList((int) (index - base), log.size()));
            } else {
                log = new ArrayList<>();
                durable = index;
                epoch++;
            }
            base = index;
            baseTerm = lastTerm;
            commit = applied = index;
            persistWith(lastIndex() + 1, List.of(), true, index, lastTerm);
            Effect e = new Effect(); e.kind = EffectKind.RESTORE; e.index = index; e.term = lastTerm;
            out.add(e);
            sendGated(from, Msg.appendReply(replyTerm, true, index));
        }

        /** The disk finished the write with {@code token} (and all earlier ones). */
        public void persisted(long token) {
            while (!writes.isEmpty() && writes.peekFirst().token <= token) {
                Write w = writes.pollFirst();
                if (w.epoch == epoch) durable = Math.min(Math.max(durable, w.upto), lastIndex());
                out.addAll(w.gated);
            }
            if (role == Role.LEADER) tryCommit();
        }

        public void step(int from, Msg m) {
            switch (m.kind) {
                case VOTE -> onVote(from, m.term, m.lastIndex, m.lastTerm, m.pre);
                case VOTE_REPLY -> onVoteReply(from, m.term, m.granted, m.pre);
                case APPEND -> onAppend(from, m.term, m.prevIndex, m.prevTerm, m.entries, m.commit);
                case APPEND_REPLY -> onAppendReply(from, m.term, m.success, m.index);
            }
        }

        // ---- helpers ----
        private int pos(int nid) { for (int i = 0; i < peers.length; i++) if (peers[i] == nid) return i; return -1; }
        private int quorum() { return peers.length / 2 + 1; }
        private long lastTerm() { return log.isEmpty() ? baseTerm : log.get(log.size() - 1).term(); }
        private long termAt(long i) {
            if (i == base) return baseTerm;
            Entry e = entryAt(i);
            return e == null ? 0 : e.term();
        }
        private void resetTimeout() {
            electionElapsed = 0;
            long t = cfg.electionTicks();
            electionTimeout = t + rng.below(t);
        }
        private List<Integer> others() {
            List<Integer> v = new ArrayList<>();
            for (int p : peers) if (p != id) v.add(p);
            return v;
        }
        private void send(int to, Msg m) {
            Effect e = new Effect(); e.kind = EffectKind.SEND; e.to = to; e.msg = m;
            out.add(e);
        }
        private void sendGated(int to, Msg m) {
            Effect e = new Effect(); e.kind = EffectKind.SEND; e.to = to; e.msg = m;
            if (!writes.isEmpty()) writes.peekLast().gated.add(e);
            else out.add(e);
        }
        private void persist(long from, List<Entry> entries) { persistWith(from, entries, false, 0, 0); }
        private void persistWith(long from, List<Entry> entries, boolean hasCompact, long cidx, long cterm) {
            long tok = nextTok++;
            writes.addLast(new Write(tok, from - 1 + entries.size(), epoch));
            Persist p = new Persist();
            p.token = tok; p.term = term; p.votedFor = votedFor; p.from = from; p.entries = entries;
            p.hasCompact = hasCompact; p.compactIndex = cidx; p.compactTerm = cterm;
            Effect e = new Effect(); e.kind = EffectKind.PERSIST; e.persist = p;
            out.add(e);
        }
        private void persistHardState() { persist(lastIndex() + 1, List.of()); }
        private void truncateFrom(long index) {
            if (index <= commit) fault = "truncating a committed entry";
            log = new ArrayList<>(log.subList(0, (int) (index - base - 1)));
            durable = Math.min(durable, index - 1);
            epoch++;
        }
        private void becomeFollower(long t, int ld) {
            if (t > term) { term = t; votedFor = NONE; persistHardState(); }
            role = Role.FOLLOWER;
            leader = ld;
            resetTimeout();
        }
        private void applyCommitted() {
            while (applied < commit) {
                applied++;
                Effect e = new Effect(); e.kind = EffectKind.APPLY; e.index = applied; e.entry = entryAt(applied);
                out.add(e);
            }
        }

        // ---- elections ----
        private void startElection() {
            if (cfg.preVote() && peers.length > 1) {
                role = Role.PRE_CANDIDATE;
                leader = NONE;
                Arrays.fill(votes, false);
                votes[pos(id)] = true;
                resetTimeout();
                for (int p : others()) send(p, Msg.vote(term + 1, lastIndex(), lastTerm(), true));
            } else {
                campaign();
            }
        }
        private int voteCount() { int c = 0; for (boolean v : votes) if (v) c++; return c; }
        private void campaign() {
            term++;
            votedFor = id;
            role = Role.CANDIDATE;
            leader = NONE;
            Arrays.fill(votes, false);
            votes[pos(id)] = true;
            persistHardState();
            resetTimeout();
            for (int p : others()) sendGated(p, Msg.vote(term, lastIndex(), lastTerm(), false));
            if (voteCount() >= quorum()) becomeLeader();
        }
        private void becomeLeader() {
            role = Role.LEADER;
            leader = id;
            long next = lastIndex() + 1;
            Arrays.fill(nextIndex, next);
            Arrays.fill(matchIndex, 0);
            Arrays.fill(heard, false);
            heartbeatElapsed = 0;
            electionElapsed = 0;
            leaderAppend(new Entry(term, new byte[0])); // the term's no-op
            broadcastAppend();
        }
        private boolean upToDate(long lastIdx, long lastT) {
            return lastT != lastTerm() ? lastT > lastTerm() : lastIdx >= lastIndex();
        }
        private void onVote(int from, long t, long lastIdx, long lastT, boolean pre) {
            boolean utd = upToDate(lastIdx, lastT);
            if (pre) {
                boolean live = role == Role.LEADER || (leader != NONE && electionElapsed < electionTimeout);
                boolean granted = t > term && utd && !live;
                send(from, Msg.voteReply(granted ? t : term, granted, true));
                return;
            }
            if (t < term) { send(from, Msg.voteReply(term, false, false)); return; }
            if (t > term) becomeFollower(t, NONE);
            boolean free = votedFor == NONE || votedFor == from;
            boolean granted = free && utd;
            if (granted) {
                if (votedFor != from) { votedFor = from; persistHardState(); }
                resetTimeout();
            }
            sendGated(from, Msg.voteReply(term, granted, false));
        }
        private void onVoteReply(int from, long t, boolean granted, boolean pre) {
            if (pre) {
                if (role != Role.PRE_CANDIDATE) return;
                if (!granted) { if (t > term) becomeFollower(t, NONE); return; }
                if (t != term + 1) return;
                votes[pos(from)] = true;
                if (voteCount() >= quorum()) campaign();
                return;
            }
            if (t > term) { becomeFollower(t, NONE); return; }
            if (role != Role.CANDIDATE || t < term || !granted) return;
            votes[pos(from)] = true;
            if (voteCount() >= quorum()) becomeLeader();
        }

        // ---- replication ----
        private void leaderAppend(Entry e) {
            log.add(e);
            persist(lastIndex(), List.of(e));
        }
        private void broadcastAppend() { for (int p : others()) sendAppend(p); }
        private void sendAppend(int to) {
            int i = pos(to);
            long next = Math.max(nextIndex[i], 1);
            if (next <= base) {
                if (snapWait[i] == 0) {
                    snapWait[i] = cfg.electionTicks() * 2;
                    Effect e = new Effect(); e.kind = EffectKind.NEED_SNAPSHOT; e.to = to; e.index = base; e.term = baseTerm;
                    out.add(e);
                }
                return;
            }
            long prev = next - 1;
            int start = (int) (prev - base);
            int end = Math.min(log.size(), start + cfg.maxBatch());
            List<Entry> es = new ArrayList<>(log.subList(start, end));
            long sent = es.size();
            Msg m = Msg.append(term, prev, termAt(prev), es, commit);
            nextIndex[i] = next + sent;
            send(to, m);
        }
        private void onAppend(int from, long t, long prevIndex, long prevTerm, List<Entry> entriesIn, long leaderCommit) {
            if (t < term) { send(from, Msg.appendReply(term, false, 0)); return; }
            becomeFollower(t, from);
            List<Entry> entries = entriesIn;
            if (prevIndex < base) {
                int skip = (int) Math.min(base - prevIndex, entries.size());
                entries = entries.subList(skip, entries.size());
                prevIndex += skip;
                if (prevIndex < base) { sendGated(from, Msg.appendReply(term, true, base)); return; }
                prevTerm = baseTerm;
            }
            if (prevIndex > lastIndex() || termAt(prevIndex) != prevTerm) {
                long hint = Math.min(lastIndex(), prevIndex > 0 ? prevIndex - 1 : 0);
                sendGated(from, Msg.appendReply(term, false, hint));
                return;
            }
            long idx = prevIndex;
            int firstNew = -1;
            for (int k = 0; k < entries.size(); k++) {
                idx++;
                if (idx <= lastIndex()) {
                    if (termAt(idx) != entries.get(k).term()) { truncateFrom(idx); firstNew = k; break; }
                } else {
                    firstNew = k;
                    break;
                }
            }
            if (firstNew >= 0) {
                long fromIndex = prevIndex + 1 + firstNew;
                List<Entry> fresh = new ArrayList<>(entries.subList(firstNew, entries.size()));
                log.addAll(fresh);
                persist(fromIndex, fresh);
            }
            long matched = prevIndex + entries.size();
            if (leaderCommit > commit) { commit = Math.min(leaderCommit, matched); applyCommitted(); }
            sendGated(from, Msg.appendReply(term, true, matched));
        }
        private void onAppendReply(int from, long t, boolean success, long index) {
            if (t > term) { becomeFollower(t, NONE); return; }
            if (role != Role.LEADER || t < term) return;
            int i = pos(from);
            heard[i] = true;
            if (success) {
                matchIndex[i] = Math.max(matchIndex[i], index);
                nextIndex[i] = Math.max(nextIndex[i], index + 1);
                tryCommit();
                if (nextIndex[i] <= lastIndex()) sendAppend(from);
            } else {
                nextIndex[i] = Math.max(Math.min(nextIndex[i] - 1, index + 1), 1);
                sendAppend(from);
            }
        }
        private void tryCommit() {
            int me = pos(id);
            long[] m = new long[peers.length];
            for (int i = 0; i < m.length; i++) m[i] = i == me ? durable : matchIndex[i];
            Arrays.sort(m);                       // ascending
            long idx = m[m.length - quorum()];    // quorum()-th largest
            // Only current-term entries are committed by counting (Raft figure 8).
            if (idx > commit && termAt(idx) == term) { commit = idx; applyCommitted(); }
        }
    }
}
