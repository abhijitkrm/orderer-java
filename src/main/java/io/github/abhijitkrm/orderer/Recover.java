//! Recovery (spec/JOURNAL.md §4–5): restore a snapshot into per-partition
//! cores under the restoring pipeline's routing, then replay command journals
//! merged by iseq.
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.orderer.Core.MatchingCore;
import io.github.abhijitkrm.orderer.Journal.CmdRecord;
import io.github.abhijitkrm.orderer.Pipeline.Snapshot;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Recover {
    private Recover() {}

    public static final class RecoverError extends RuntimeException {
        public RecoverError(String m) { super(m); }
    }

    /// A snapshot body + its .meta sidecar (missing sidecar ⇒ cut 0, e.g. a matcher snapshot).
    public static Snapshot readSnapshot(Path path) {
        String body;
        try { body = Files.readString(path, StandardCharsets.UTF_8); }
        catch (IOException e) { throw new RecoverError("snapshot: " + path + ": cannot read"); }
        long iseq = 0;
        int partitions = 1;
        Path mp = Pipeline.metaPath(path);
        if (Files.exists(mp)) {
            String line;
            try {
                String t = Files.readString(mp, StandardCharsets.UTF_8);
                int nl = t.indexOf('\n');
                line = nl < 0 ? t : t.substring(0, nl);
            } catch (IOException e) { throw new RecoverError("snapshot: " + mp + ": bad sidecar"); }
            Long i = Flat.u64(line, "iseq");
            if (!"orderer-meta/1".equals(Flat.get(line, "format")) || i == null)
                throw new RecoverError("snapshot: " + mp + ": bad sidecar");
            iseq = i;
            Long p = Flat.u64(line, "partitions");
            partitions = p == null ? 1 : (int) (long) p;
        }
        return new Snapshot(body, iseq, partitions);
    }

    public record Restored<C>(Config book, List<C> cores) {}

    /// Empty cores, or cores restored from `snap` (its header overrides `book`).
    public static <C extends MatchingCore> Restored<C> restore(Core.Factory<C> f, Config book, PartitionMap map, Snapshot snap) {
        List<C> cores = new ArrayList<>();
        if (snap == null) {
            for (int p = 0; p < map.partitions(); p++) cores.add(f.create(book));
            return new Restored<>(book, cores);
        }
        Core.ParsedSnapshot ps;
        try { ps = Core.parseSnapshot(snap.body()); }
        catch (Core.RestoreError e) { throw new RecoverError("snapshot: " + e.getMessage()); }
        for (int p = 0; p < map.partitions(); p++) cores.add(f.create(ps.cfg()));
        Set<Long> seen = new HashSet<>();
        for (Core.SnapBook b : ps.books()) {
            if (!seen.add(b.symbol())) throw new RecoverError("snapshot: book " + b.symbol() + " appears twice");
            String e = cores.get(map.partition(b.symbol())).restoreBook(b.symbol(), b.seq(), b.orders());
            if (e != null) throw new RecoverError("snapshot: " + e);
        }
        return new Restored<>(ps.cfg(), cores);
    }

    /// One iseq-ordered stream of records after `after`; iseqs must be disjoint.
    public static List<CmdRecord> mergeJournals(List<List<CmdRecord>> parts, long after) {
        List<CmdRecord> all = new ArrayList<>();
        for (List<CmdRecord> p : parts)
            for (CmdRecord r : p) if (Long.compareUnsigned(r.iseq(), after) > 0) all.add(r);
        all.sort((a, b) -> Long.compareUnsigned(a.iseq(), b.iseq()));
        for (int i = 1; i < all.size(); i++)
            if (all.get(i).iseq() == all.get(i - 1).iseq())
                throw new Journal.CorruptJournal("iseq " + Long.toUnsignedString(all.get(i).iseq()) + " appears in two partitions");
        return all;
    }

    public record Recovery<C>(Config book, List<C> cores, long snapshotIseq, long lastIseq, long replayed) {
        public Pipeline.Initial<C> toInitial() { return new Pipeline.Initial<>(cores, lastIseq + 1); }
    }

    @FunctionalInterface
    public interface PartitionEmit { void onEvent(int partition, long sym, long seq, Event ev); }

    public record JournalSource(Path dir, Journal.Format format) {}

    /// Snapshot (optional) + every command journal in `dir` (optional), records
    /// after the cut replayed in iseq order. The book config comes from the
    /// snapshot, else the journals, else `book`.
    public static <C extends MatchingCore> Recovery<C> recover(Core.Factory<C> f, Config book, PartitionMap map,
                                                               Snapshot snap, JournalSource journal, PartitionEmit emit) {
        Journal.CmdDir journals = journal == null ? null : Journal.readCmdDir(journal.dir(), journal.format());
        if (journals != null && snap == null) book = journals.header().book();
        Restored<C> r = restore(f, book, map, snap);
        if (journals != null && !Flat.sameBook(journals.header().book(), r.book()))
            throw new RecoverError("snapshot: snapshot and journal book configs differ");
        long cut = snap == null ? 0 : snap.iseq();
        List<CmdRecord> recs = journals == null ? List.of() : mergeJournals(journals.partitions(), cut);
        long last = recs.isEmpty() ? cut : recs.get(recs.size() - 1).iseq();
        if (Long.compareUnsigned(cut, last) > 0) last = cut;
        for (CmdRecord rec : recs) {
            int p = map.partition(rec.sym());
            r.cores().get(p).apply(rec.sym(), rec.cmd(), (s, seq, ev) -> emit.onEvent(p, s, seq, ev));
        }
        return new Recovery<>(r.book(), r.cores(), cut, last, recs.size());
    }
}
