// Integration suite (orderer-rust tests/{partitions,journal_recovery,
// control,plugs}.rs, ported).
//   PipelineTest [scratch-dir]
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.OType;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.Journal.CmdRecord;
import io.github.abhijitkrm.orderer.Journal.Format;
import io.github.abhijitkrm.orderer.Journal.FsyncPolicy;
import io.github.abhijitkrm.orderer.Pipeline.PipelineError;
import io.github.abhijitkrm.orderer.Pipeline.Snapshot;
import io.github.abhijitkrm.orderer.Pipeline.Status;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class PipelineTest {
    static Path SCRATCH = Path.of("out-test/scratch");
    static final Config CFG = T.fuzzCfg();

    // ---- partitions ----------------------------------------------------------------------------

    static void everyPartitionCountMatchesReferencePerSymbol() {
        for (long seed = 1; seed <= 8; seed++) {
            T.Cmds cmds = T.fuzzCorpus(seed, 4000, 8);
            List<String> ref = T.referenceLines(CFG, cmds);
            for (int P : new int[] {1, 2, 3, 4, 7}) {
                List<List<String>> parts = T.runPipeline(CFG, cmds, P);
                if (P == 1) check(parts.get(0).equals(ref), "seed ", seed, ": P=1 is the plain engine stream");
                List<String> all = T.concat(parts);
                check(T.dense(all), "seq density");
                check(T.bySymbol(all).equals(T.bySymbol(ref)), "seed ", seed, " P=", P);
                for (int q = 0; q < parts.size(); q++)
                    for (String l : parts.get(q)) check(Routing.hashPartition(Flat.u64(l, "symbol"), P) == q);
            }
        }
    }

    static void fuzzRunsAreDeterministicPerPartition() {
        for (long seed = 1; seed <= 8; seed++) {
            T.Cmds cmds = T.fuzzCorpus(seed, 3000, 8);
            check(T.runPipeline(CFG, cmds, 4).equals(T.runPipeline(CFG, cmds, 4)), seed);
        }
    }

    static void partitionTableRoutesSymbols() {
        T.Cmds cmds = T.fuzzCorpus(11, 3000, 8);
        List<long[]> table = new ArrayList<>();
        for (long s = 0; s < 8; s++) table.add(new long[] {s, s == 5 ? 0 : 2});
        PartitionMap m = new PartitionMap(3, table);
        Egress.Collect col = Egress.collect(true);
        try (var p = Pipeline.builder().bookConfig(CFG).partitionMap(m).egress(col.factory()).build()) {
            p.publishBatch(cmds.symArray(), cmds.cmdArray());
            p.drain();
        }
        List<List<String>> parts = new ArrayList<>();
        for (String b : col.handle().take()) parts.add(T.lines(b));
        check(parts.get(1).isEmpty());
        for (String l : parts.get(0)) check(Flat.u64(l, "symbol") == 5);
        check(T.bySymbol(T.concat(parts)).equals(T.bySymbol(T.referenceLines(CFG, cmds))));
    }

    static void manyProducersPreservePerSymbolOrder() throws Exception {
        T.Cmds cmds = T.fuzzCorpus(3, 20000, 16);
        Egress.Collect col = Egress.collect(true);
        try (var p = Pipeline.builder().bookConfig(CFG).partitions(4).ringSizes(256, 64, 64).egress(col.factory()).build()) {
            List<Thread> ts = new ArrayList<>();
            for (int k = 0; k < 4; k++) {
                T.Cmds mine = new T.Cmds();
                for (int i = 0; i < cmds.size(); i++) if (cmds.syms.get(i) % 4 == k) mine.add(cmds.syms.get(i), cmds.cmds.get(i));
                long[] sy = mine.symArray();
                Command[] cm = mine.cmdArray();
                Pipeline.Handle h = p.handle();
                Thread t = new Thread(() -> {
                    for (int i = 0; i < cm.length; i += 7) {
                        int n = Math.min(7, cm.length - i);
                        if (n % 2 == 0) h.publishBatch(sy, cm, i, n);
                        else for (int j = 0; j < n; j++) h.publish(sy[i + j], cm[i + j]);
                    }
                    h.close();
                });
                t.start();
                ts.add(t);
            }
            for (Thread t : ts) t.join();
            p.drain();
        }
        check(T.bySymbol(T.lines(col.handle().listing())).equals(T.bySymbol(T.referenceLines(CFG, cmds))));
    }

    // ---- journals + recovery ---------------------------------------------------------------------

    static Journal.Config2 jcfg(Path d, Format f) { return Journal.Config2.of(d, f).withFsync(FsyncPolicy.everyN(64)); }

    static void journalsSnapshotAndRecoveryRoundTrip() {
        for (Format fmt : Format.values()) {
            for (int P : new int[] {1, 3}) {
                Path dir = T.scratch(SCRATCH, "jr-" + fmt + "-" + P);
                T.Cmds cmds = T.fuzzCorpus(21 + P, 5000, 8);
                final int cut = 2000;
                Egress.Collect col = Egress.collect(true);
                T.Cmds first = cmds.slice(0, cut), second = cmds.slice(cut, cmds.size());
                Snapshot snap;
                try (var p = Pipeline.builder().bookConfig(CFG).partitions(P).ringSizes(512, 128, 128)
                        .journal(jcfg(dir, fmt)).egress(col.factory()).build()) {
                    p.publishBatch(first.symArray(), first.cmdArray());
                    snap = p.snapshot();
                    p.publishBatch(second.symArray(), second.cmdArray());
                }
                List<String> allRef = T.referenceLines(CFG, cmds);
                int prefixLen = T.referenceLines(CFG, first).size();
                check(snap.iseq() == cut);
                check(snap.body().equals(T.referenceSnapshot(CFG, cmds, cut)), "snapshot body");
                Journal.CmdDir jd = Journal.readCmdDir(dir, fmt);
                check(jd.header().partitions() == P && Flat.sameBook(jd.header().book(), CFG));
                List<CmdRecord> merged = new ArrayList<>();
                for (int q = 0; q < jd.partitions().size(); q++)
                    for (CmdRecord r : jd.partitions().get(q)) { check(Routing.hashPartition(r.sym(), P) == q); merged.add(r); }
                merged.sort((a, b) -> Long.compare(a.iseq(), b.iseq()));
                check(merged.size() == cmds.size());
                for (int i = 0; i < merged.size() && i < cmds.size(); i++)
                    check(merged.get(i).iseq() == i + 1 && merged.get(i).sym() == cmds.syms.get(i), i);
                List<String> collected = col.handle().take();
                for (int q = 0; q < P; q++)
                    check(Journal.readEvt(Journal.path(dir, Journal.Kind.Evt, q, fmt), fmt).equals(T.lines(collected.get(q))), "evt-", q);
                for (int rp : new int[] {P, 2}) {
                    List<String> replayed = new ArrayList<>();
                    var rec = Recover.recover(Core.FifoCore::new, CFG, new PartitionMap(rp), snap,
                            new Recover.JournalSource(dir, fmt),
                            (q, s, seq, e) -> replayed.add(io.github.abhijitkrm.matcher.Types.Event.canonical(seq, s, e)));
                    check(rec.snapshotIseq() == cut && rec.lastIseq() == cmds.size() && rec.replayed() == cmds.size() - cut);
                    check(allRef.subList(prefixLen, allRef.size()).equals(replayed), "recover P=", P, " → ", rp);
                }
            }
        }
    }

    static void recoveredPipelineResumesAndAppends() {
        Path dir = T.scratch(SCRATCH, "resume");
        Journal.Config2 j = jcfg(dir, Format.Binary);
        T.Cmds cmds = T.fuzzCorpus(77, 4000, 6);
        T.Cmds first = cmds.slice(0, 2500), second = cmds.slice(2500, cmds.size());
        try (var p = Pipeline.builder().bookConfig(CFG).partitions(2).journal(j).build()) {
            p.publishBatch(first.symArray(), first.cmdArray());
        }
        PartitionMap m = new PartitionMap(2);
        var rec = Recover.recover(Core.FifoCore::new, CFG, m, null, new Recover.JournalSource(dir, Format.Binary),
                (q, s, seq, e) -> {});
        check(rec.lastIseq() == 2500);
        Egress.Collect col = Egress.collect(true);
        Snapshot snap;
        try (var p = Pipeline.builder().bookConfig(rec.book()).partitionMap(m).journal(j.withAppend(true))
                .egress(col.factory()).initial(rec.toInitial()).build()) {
            p.publishBatch(second.symArray(), second.cmdArray());
            p.drain();
            snap = p.snapshot();
        }
        List<String> allRef = T.referenceLines(CFG, cmds);
        int prefixLen = T.referenceLines(CFG, first).size();
        check(T.bySymbol(T.lines(col.handle().listing())).equals(T.bySymbol(allRef.subList(prefixLen, allRef.size()))));
        check(snap.iseq() == 4000, "iseq resumed");
        check(snap.body().equals(T.referenceSnapshot(CFG, cmds, cmds.size())));
        long total = 0;
        for (var r : Journal.readCmdDir(dir, Format.Binary).partitions()) total += r.size();
        check(total == 4000, "appended journals hold the whole history");
    }

    static void tornAndCorruptJournalsAreErrors() throws Exception {
        T.Cmds cmds = T.fuzzCorpus(8, 500, 1);
        for (Format fmt : Format.values()) {
            Path dir = T.scratch(SCRATCH, "torn-" + fmt);
            try (var p = Pipeline.builder().bookConfig(CFG).journal(jcfg(dir, fmt)).build()) {
                p.publishBatch(cmds.symArray(), cmds.cmdArray());
            }
            Path path = Journal.path(dir, Journal.Kind.Cmd, 0, fmt);
            byte[] good = Files.readAllBytes(path);
            class Expect {
                void corrupt(byte[] bytes, String what) throws Exception {
                    Files.write(path, bytes);
                    boolean threw = false;
                    try { Journal.readCmdDir(dir, fmt); }
                    catch (Journal.CorruptJournal e) { threw = true; check(e.getMessage().contains(what), e.getMessage()); }
                    check(threw, what);
                }
            }
            Expect x = new Expect();
            x.corrupt(Arrays.copyOf(good, good.length - 7), "torn");
            byte[] back;
            if (fmt == Format.Binary) {
                back = good.clone();
                int off = back.length - Journal.CMD_RECORD;
                for (int i = 0; i < 8; i++) back[off + i] = (byte) (i == 0 ? 1 : 0);
                // a well-formed (resealed) record, just out of order
                java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(back).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                bb.putInt(off + Journal.CMD_RECORD_V1, Journal.crc32c(bb, off, Journal.CMD_RECORD_V1));
            } else {
                byte[] extra = "{\"cmd\":\"cancel\",\"symbol\":0,\"order_id\":1,\"iseq\":3}\n".getBytes();
                back = Arrays.copyOf(good, good.length + extra.length);
                System.arraycopy(extra, 0, back, good.length, extra.length);
            }
            x.corrupt(back, "iseq");
            byte[] hdr = good.clone();
            hdr[2] = '#';
            x.corrupt(hdr, "");
            Files.write(path, good);
            try { Journal.readCmdDir(dir, fmt); } catch (RuntimeException e) { check(false, "good journal rejected: ", e); }
        }
    }

    // ---- controls --------------------------------------------------------------------------------

    static void snapshotsUnderLoadAreCleanCuts() throws Exception {
        T.Cmds cmds = T.fuzzCorpus(31, 30000, 8);
        long[] sy = cmds.symArray();
        Command[] cm = cmds.cmdArray();
        List<Snapshot> snaps = new ArrayList<>();
        try (var p = Pipeline.builder().bookConfig(CFG).partitions(3).ringSizes(256, 64, 64).build()) {
            Pipeline.Handle h = p.handle();
            Thread prod = new Thread(() -> {
                for (int i = 0; i < cm.length; i += 50) h.publishBatch(sy, cm, i, Math.min(50, cm.length - i));
            });
            prod.start();
            while (snaps.size() < 6) { snaps.add(p.snapshot()); T.sleep(2); }
            prod.join();
            snaps.add(p.snapshot());
        }
        for (Snapshot s : snaps) check(s.body().equals(T.referenceSnapshot(CFG, cmds, s.iseq())), "cut at ", s.iseq());
        check(snaps.get(snaps.size() - 1).iseq() == cmds.size());
    }

    static Command limit(long id) { return new Command.New(id, Side.Bid, OType.Limit, 10, 1, Tif.Gtc); }

    static void shutdownIsIdempotentAndClosesPublishing() {
        var p = Pipeline.builder().partitions(2).build();
        var h = p.handle();
        p.publish(1, limit(1));
        p.shutdown();
        p.shutdown();
        check(p.publish(1, new Command.Cancel(1)) == Status.Closed);
        check(h.publish(1, new Command.Cancel(1)) == Status.Closed);
        check(h.tryPublish(1, new Command.Cancel(1)) == Status.Closed);
        boolean threw = false;
        try { p.drain(); } catch (PipelineError e) { threw = e.kind == PipelineError.Kind.Closed; }
        check(threw, "drain after shutdown");
    }

    static void everyOkPublishRacingShutdownIsApplied() throws Exception {
        for (int round = 0; round < 5; round++) {
            Egress.Collect col = Egress.collect(true);
            var p = Pipeline.builder(Core.NoopCore::new).partitions(2).ringSizes(64, 16, 16).egress(col.factory()).build();
            AtomicLong accepted = new AtomicLong();
            List<Thread> ts = new ArrayList<>();
            for (int k = 0; k < 3; k++) {
                int kk = k;
                Pipeline.Handle h = p.handle();
                Thread t = new Thread(() -> {
                    for (long i = 0; h.publish(kk, new Command.Cancel(i)) == Status.Ok; i++) accepted.incrementAndGet();
                });
                t.start();
                ts.add(t);
            }
            T.sleep(5);
            p.shutdown();
            for (Thread t : ts) t.join();
            check(T.lines(col.handle().listing()).size() == accepted.get(), "Ok ⇒ applied");
        }
    }

    static void acksWaitForFsync() {
        T.Cmds cmds = T.fuzzCorpus(4, 2000, 6);
        int total = T.referenceLines(CFG, cmds).size();
        for (boolean noneBeforeShutdown : new boolean[] {true, false}) {
            Path dir = T.scratch(SCRATCH, "acks");
            FsyncPolicy pol = noneBeforeShutdown ? FsyncPolicy.every(3_600_000_000_000L)
                    : FsyncPolicy.everyN(1L << 40).withIdle(20_000_000);
            Journal.Config2 j = jcfg(dir, Format.Binary).withFsync(pol).withEvents(false);
            AtomicLong acked = new AtomicLong();
            var p = Pipeline.builder().bookConfig(CFG).partitions(2).journal(j)
                    .egress(Egress.acks((q, m) -> acked.incrementAndGet())).build();
            p.publishBatch(cmds.symArray(), cmds.cmdArray());
            p.drain();
            if (noneBeforeShutdown) {
                T.sleep(100);
                check(acked.get() == 0, "acked before any fsync");
                check(p.durableIseq(0) == 0 && p.durableIseq(1) == 0);
            } else {
                long deadline = System.nanoTime() + 20_000_000_000L;
                while (acked.get() < total && System.nanoTime() < deadline) T.sleep(5);
                check(Math.max(p.durableIseq(0), p.durableIseq(1)) == cmds.size());
            }
            p.shutdown();
            check(acked.get() == total, acked.get(), " of ", total);
        }
    }

    static Egress.Factory slow() {
        return ctx -> m -> { long until = System.nanoTime() + 20_000; while (System.nanoTime() < until) Thread.onSpinWait(); };
    }

    static void tinyRingsAndSlowEgressBlockWithoutLoss() {
        T.Cmds cmds = T.fuzzCorpus(17, 3000, 4);
        Egress.Collect col = Egress.collect(true);
        try (var p = Pipeline.builder().bookConfig(CFG).partitions(2).ringSizes(2, 2, 2).egress(slow()).egress(col.factory()).build()) {
            for (int i = 0; i < cmds.size(); i++) check(p.publish(cmds.syms.get(i), cmds.cmds.get(i)) == Status.Ok);
            p.drain();
        }
        check(T.bySymbol(T.lines(col.handle().listing())).equals(T.bySymbol(T.referenceLines(CFG, cmds))));
    }

    static void tryPublishShedsAtTheEdgeOnly() {
        Egress.Collect col = Egress.collect(true);
        long ok = 0, full = 0;
        try (var p = Pipeline.builder(Core.NoopCore::new).partitions(1).ringSizes(4, 2, 2).egress(slow()).egress(col.factory()).build()) {
            for (long i = 0; i < 2000; i++) {
                Status s = p.tryPublish(1, new Command.Cancel(i));
                if (s == Status.Ok) ok++;
                else if (s == Status.Full) full++;
            }
            p.drain();
        }
        check(full > 0);
        List<String> got = T.lines(col.handle().listing());
        check(got.size() == ok && T.dense(got));
    }

    // ---- plugs -----------------------------------------------------------------------------------

    static void noopCoreSeesEveryCommand() {
        T.Cmds cmds = T.fuzzCorpus(5, 5000, 8);
        List<String> got = T.concat(T.runPipeline(Core.NoopCore::new, CFG, cmds.symArray(), cmds.cmdArray(), 3, true));
        check(got.size() == cmds.size() && T.dense(got));
    }

    /// A core that throws on a poison order id.
    static final class PanicCore implements Core.MatchingCore {
        final Core.FifoCore inner;
        PanicCore(Config c) { inner = new Core.FifoCore(c); }
        public void apply(long s, Command c, Core.Emit emit) {
            if (c instanceof Command.Cancel x && x.orderId() == 666) throw new IllegalStateException("poison command");
            inner.apply(s, c, emit);
        }
        public void snapshotBlocks(List<Core.Block> b) { inner.snapshotBlocks(b); }
        public String restoreBook(long s, long q, List<io.github.abhijitkrm.matcher.OrderBook.RestingOrder> o) {
            return inner.restoreBook(s, q, o);
        }
    }

    static void failingCoreFailsThePipelineInsteadOfHanging() {
        var p = Pipeline.builder(PanicCore::new).partitions(2).build();
        p.publish(1, limit(1));
        p.publish(1, new Command.Cancel(666));
        boolean failed = false;
        try { p.drain(); }
        catch (PipelineError e) { failed = e.kind == PipelineError.Kind.Failed && e.getMessage().contains("engine"); }
        check(failed);
        failed = false;
        try { p.shutdown(); } catch (PipelineError e) { failed = e.kind == PipelineError.Kind.Failed; }
        check(failed);
    }

    // ---- 1.2: checksums, repair, checkpoints -------------------------------------------------

    static void crc32cMatchesTheSpecCheckValue() {
        java.nio.ByteBuffer b = java.nio.ByteBuffer.wrap("123456789".getBytes());
        check(Journal.crc32c(b, 0, 9) == 0xE3069283, "check value");
    }

    static void checksumsCatchFlippedBitsAnywhere() throws Exception {
        Path dir = T.scratch(SCRATCH, "crc");
        T.Cmds cmds = T.fuzzCorpus(9, 300, 2);
        try (var p = Pipeline.builder().bookConfig(CFG).journal(jcfg(dir, Format.Binary)).build()) {
            p.publishBatch(cmds.symArray(), cmds.cmdArray());
        }
        Path path = Journal.path(dir, Journal.Kind.Cmd, 0, Format.Binary);
        byte[] bad = Files.readAllBytes(path);
        bad[Journal.HEADER + 100 * Journal.CMD_RECORD + 20] ^= 0x10;
        Files.write(path, bad);
        boolean threw = false;
        try { Journal.readCmdDir(dir, Format.Binary); }
        catch (Journal.CorruptJournal e) { threw = e.getMessage().contains("checksum"); }
        check(threw, "strict");
        threw = false;
        try { Journal.repairDir(dir, Format.Binary); } catch (Journal.CorruptJournal e) { threw = true; }
        check(threw, "mid-file damage is not repairable");
    }

    static void repairCutsOnlyATornTail() throws Exception {
        T.Cmds cmds = T.fuzzCorpus(10, 400, 3);
        for (Format fmt : Format.values()) {
            Path dir = T.scratch(SCRATCH, "repair-" + fmt);
            try (var p = Pipeline.builder().bookConfig(CFG).journal(jcfg(dir, fmt)).build()) {
                p.publishBatch(cmds.symArray(), cmds.cmdArray());
            }
            Path path = Journal.path(dir, Journal.Kind.Cmd, 0, fmt);
            byte[] good = Files.readAllBytes(path);
            List<CmdRecord> full = Journal.readCmdDir(dir, fmt).partitions().get(0);
            Files.write(path, Arrays.copyOf(good, good.length - 5));
            boolean threw = false;
            try { Journal.readCmdDir(dir, fmt); } catch (Journal.CorruptJournal e) { threw = true; }
            check(threw, "strict rejects a torn tail");
            check(Journal.repairDir(dir, fmt).size() == 1);
            check(Journal.readCmdDir(dir, fmt).partitions().get(0).equals(full.subList(0, full.size() - 1)), "a prefix survives");
            if (fmt == Format.Binary) {
                byte[] zeroed = good.clone();
                Arrays.fill(zeroed, zeroed.length - Journal.CMD_RECORD, zeroed.length, (byte) 0);
                Files.write(path, zeroed);
                check(Journal.repairDir(dir, fmt).size() == 1, "a complete record that never reached the disk");
                check(Journal.readCmdDir(dir, fmt).partitions().get(0).size() == full.size() - 1);
            }
            check(Journal.repairDir(dir, fmt).isEmpty(), "a clean file is left alone");
        }
    }

    static void checkpointsRotateSegmentsAndBoundRecovery() {
        T.Cmds cmds = T.fuzzCorpus(12, 3000, 6);
        for (Format fmt : Format.values()) {
            Path dir = T.scratch(SCRATCH, "ckpt-" + fmt);
            Egress.Collect col = Egress.collect(true);
            Snapshot c1, c2;
            try (var p = Pipeline.builder().bookConfig(CFG).partitions(3).journal(jcfg(dir, fmt)).egress(col.factory()).build()) {
                T.Cmds a = cmds.slice(0, 1000), b = cmds.slice(1000, 2200), c = cmds.slice(2200, cmds.size());
                p.publishBatch(a.symArray(), a.cmdArray());
                c1 = p.checkpoint();
                p.publishBatch(b.symArray(), b.cmdArray());
                c2 = p.checkpoint();
                p.publishBatch(c.symArray(), c.cmdArray());
            }
            check(c1.iseq() == 1000 && c2.iseq() == 2200);
            List<Journal.Checkpoint> cps = Journal.listCheckpoints(dir);
            check(cps.size() == 1 && cps.get(0).cut() == 2200, "only the last checkpoint remains");
            for (Journal.Kind k : Journal.Kind.values()) {
                List<Journal.Segment> segs = Journal.listSegments(dir, k, fmt);
                check(segs.size() == 3);
                for (Journal.Segment s : segs) check(s.start() == 2200, s.path());
            }
            check(c2.body().equals(T.referenceSnapshot(CFG, cmds, 2200)));
            List<String> replayed = new ArrayList<>();
            var rec = Recover.recover(Core.FifoCore::new, CFG, new PartitionMap(3), Recover.readSnapshot(cps.get(0).path()),
                    new Recover.JournalSource(dir, fmt),
                    (q, s, seq, e) -> replayed.add(io.github.abhijitkrm.matcher.Types.Event.canonical(seq, s, e)));
            List<String> all = T.referenceLines(CFG, cmds);
            int prefix = T.referenceLines(CFG, cmds.slice(0, 2200)).size();
            check(rec.replayed() == cmds.size() - 2200);
            check(all.subList(prefix, all.size()).equals(replayed), "recover from the checkpoint");
            List<String> evts = new ArrayList<>();
            for (int q = 0; q < 3; q++) evts.addAll(Journal.readEvtPartition(dir, fmt, q));
            List<String> want = new ArrayList<>(all.subList(prefix, all.size()));
            java.util.Collections.sort(evts);
            java.util.Collections.sort(want);
            check(evts.equals(want), "event segments hold the tail");
            check(T.lines(col.handle().listing()).size() == all.size());
        }
    }

    static void appendContinuesTheLastSegmentAfterACheckpoint() {
        T.Cmds cmds = T.fuzzCorpus(14, 2000, 4);
        Path dir = T.scratch(SCRATCH, "ckpt-append");
        Journal.Config2 j = jcfg(dir, Format.Binary);
        try (var p = Pipeline.builder().bookConfig(CFG).partitions(2).journal(j).build()) {
            T.Cmds a = cmds.slice(0, 800), b = cmds.slice(800, 1200);
            p.publishBatch(a.symArray(), a.cmdArray());
            p.checkpoint();
            p.publishBatch(b.symArray(), b.cmdArray());
        }
        PartitionMap m = new PartitionMap(2);
        var rec = Recover.recover(Core.FifoCore::new, CFG, m, Recover.readSnapshot(Journal.listCheckpoints(dir).get(0).path()),
                new Recover.JournalSource(dir, Format.Binary), (q, s, seq, e) -> {});
        check(rec.lastIseq() == 1200);
        Snapshot snap;
        try (var p = Pipeline.builder().bookConfig(rec.book()).partitionMap(m).journal(j.withAppend(true))
                .initial(rec.toInitial()).build()) {
            T.Cmds c = cmds.slice(1200, cmds.size());
            p.publishBatch(c.symArray(), c.cmdArray());
            snap = p.snapshot();
        }
        check(snap.iseq() == 2000 && snap.body().equals(T.referenceSnapshot(CFG, cmds, 2000)));
        long total = 0;
        for (var r : Journal.readCmdDir(dir, Format.Binary).partitions()) total += r.size();
        check(total == 1200, "800 checkpointed away");
    }

    static void automaticCheckpointsKeepTheDirectoryRecoverable() {
        T.Cmds cmds = T.fuzzCorpus(15, 20000, 8);
        Path dir = T.scratch(SCRATCH, "ckpt-auto");
        Snapshot last;
        try (var p = Pipeline.builder().bookConfig(CFG).partitions(3).journal(jcfg(dir, Format.Binary))
                .checkpointEvery(20_000_000).build()) {
            long[] sy = cmds.symArray();
            Command[] cm = cmds.cmdArray();
            Pipeline.Handle h = p.handle();
            for (int i = 0; i < cm.length; i += 500) {
                h.publishBatch(sy, cm, i, Math.min(500, cm.length - i));
                T.sleep(3);
            }
            h.close();
            last = p.snapshot();
        }
        List<Journal.Checkpoint> cps = Journal.listCheckpoints(dir);
        check(cps.size() == 1 && cps.get(0).cut() > 0, "one automatic checkpoint remains");
        PartitionMap m = new PartitionMap(3);
        var rec = Recover.recover(Core.FifoCore::new, CFG, m, Recover.readSnapshot(cps.get(0).path()),
                new Recover.JournalSource(dir, Format.Binary), (q, s, seq, e) -> {});
        check(rec.lastIseq() == cmds.size());
        try (var p2 = Pipeline.builder().bookConfig(rec.book()).partitionMap(m).initial(rec.toInitial()).build()) {
            check(p2.snapshot().body().equals(last.body()), "the directory recovers the final state");
        }
        boolean threw = false;
        try { Pipeline.builder().checkpointEvery(5_000_000).build(); }
        catch (PipelineError e) { threw = e.kind == PipelineError.Kind.Config; }
        check(threw, "needs journals");
    }

    static void statsCountCommandsEventsAndFsyncs() {
        T.Cmds cmds = T.fuzzCorpus(16, 5000, 6);
        Path dir = T.scratch(SCRATCH, "stats");
        try (var p = Pipeline.builder().bookConfig(CFG).partitions(3).journal(jcfg(dir, Format.Binary)).build()) {
            p.publishBatch(cmds.symArray(), cmds.cmdArray());
            p.drain();
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (p.stats().partitions().stream().anyMatch(s -> s.fsyncs() == 0 || s.durableIseq() < s.flushedIseq()) && System.nanoTime() < deadline)
                T.sleep(5);
            Stats.PipelineStats st = p.stats();
            check(st.ingressDepth() == 0 && st.partitions().size() == 3);
            long commands = 0, events = 0, durable = 0;
            for (Stats.PartitionStats s : st.partitions()) {
                check(s.inboxDepth() == 0 && s.outboxDepth() == 0);
                check(s.fsyncs() > 0 && s.fsyncNsMax() > 0, "fsyncs on partition ", s.partition());
                commands += s.commands();
                events += s.events();
                durable = Math.max(durable, s.durableIseq());
            }
            check(commands == cmds.size() && events == T.referenceLines(CFG, cmds).size() && durable == cmds.size());
            String prom = st.toPrometheus();
            check(prom.contains("# TYPE orderer_commands_total counter"));
            check(prom.contains("orderer_inbox_depth{partition=\"2\"} 0"));
        }
    }

    public static void main(String[] args) {
        if (args.length > 0) SCRATCH = Path.of(args[0]);
        T.runAll(new Object[][] {
            {"every_partition_count_matches_reference_per_symbol", (T.Body) PipelineTest::everyPartitionCountMatchesReferencePerSymbol},
            {"fuzz_runs_are_deterministic_per_partition", (T.Body) PipelineTest::fuzzRunsAreDeterministicPerPartition},
            {"partition_table_routes_symbols", (T.Body) PipelineTest::partitionTableRoutesSymbols},
            {"many_producers_preserve_per_symbol_order", (T.Body) PipelineTest::manyProducersPreservePerSymbolOrder},
            {"journals_snapshot_and_recovery_round_trip", (T.Body) PipelineTest::journalsSnapshotAndRecoveryRoundTrip},
            {"recovered_pipeline_resumes_and_appends", (T.Body) PipelineTest::recoveredPipelineResumesAndAppends},
            {"torn_and_corrupt_journals_are_errors", (T.Body) PipelineTest::tornAndCorruptJournalsAreErrors},
            {"snapshots_under_load_are_clean_cuts", (T.Body) PipelineTest::snapshotsUnderLoadAreCleanCuts},
            {"shutdown_is_idempotent_and_closes_publishing", (T.Body) PipelineTest::shutdownIsIdempotentAndClosesPublishing},
            {"every_ok_publish_racing_shutdown_is_applied", (T.Body) PipelineTest::everyOkPublishRacingShutdownIsApplied},
            {"acks_wait_for_fsync", (T.Body) PipelineTest::acksWaitForFsync},
            {"tiny_rings_and_slow_egress_block_without_loss", (T.Body) PipelineTest::tinyRingsAndSlowEgressBlockWithoutLoss},
            {"try_publish_sheds_at_the_edge_only", (T.Body) PipelineTest::tryPublishShedsAtTheEdgeOnly},
            {"noop_core_sees_every_command", (T.Body) PipelineTest::noopCoreSeesEveryCommand},
            {"failing_core_fails_the_pipeline_instead_of_hanging", (T.Body) PipelineTest::failingCoreFailsThePipelineInsteadOfHanging},
            {"crc32c_matches_the_spec_check_value", (T.Body) PipelineTest::crc32cMatchesTheSpecCheckValue},
            {"checksums_catch_flipped_bits_anywhere", (T.Body) PipelineTest::checksumsCatchFlippedBitsAnywhere},
            {"repair_cuts_only_a_torn_tail", (T.Body) PipelineTest::repairCutsOnlyATornTail},
            {"checkpoints_rotate_segments_and_bound_recovery", (T.Body) PipelineTest::checkpointsRotateSegmentsAndBoundRecovery},
            {"append_continues_the_last_segment_after_a_checkpoint", (T.Body) PipelineTest::appendContinuesTheLastSegmentAfterACheckpoint},
            {"stats_count_commands_events_and_fsyncs", (T.Body) PipelineTest::statsCountCommandsEventsAndFsyncs},
            {"automatic_checkpoints_keep_the_directory_recoverable", (T.Body) PipelineTest::automaticCheckpointsKeepTheDirectoryRecoverable},
        });
    }
}
