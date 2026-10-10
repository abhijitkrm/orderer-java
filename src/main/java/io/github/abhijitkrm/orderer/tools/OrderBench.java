//! orderbench — spec/BENCH.md protocol.
//!   orderbench <prefix> --mode core [--tag NAME]
//!   orderbench <prefix> --mode pipe --partitions P [--producers N]
//!              [--journal binary|jsonl|off] [--journal-dir DIR] [--fsync N] [--tag NAME]
//! orderer-java tuning flags (listed in the config column when set):
//!   --core fifo|noop  --waits relaxed|low  --batch N  --ingress N --inbox N --outbox N
//!   --events on|off   --baseline OPS (core untimed ops/s, for eff)  --warmups N (JIT, default 3)
//! Core rows also report alloc_b_op= (bytes allocated per command, untimed pass).
package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.matcher.Engine;
import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.matcher.Sink;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.orderer.Core;
import io.github.abhijitkrm.orderer.Egress;
import io.github.abhijitkrm.orderer.Harness;
import io.github.abhijitkrm.orderer.Harness.Args;
import io.github.abhijitkrm.orderer.Harness.Corpus;
import io.github.abhijitkrm.orderer.Journal;
import io.github.abhijitkrm.orderer.Pipeline;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

public final class OrderBench {
    static final class Row {
        int ops;
        long wallNs;
        long[] lat = new long[0];
        Double untimed;
        Double allocPerOp;
    }

    interface Target { void apply(long sym, Command c); }

    static Target make(Corpus setup, Sink sink) {
        if (setup.engine) {
            Engine e = new Engine(setup.book);
            return (s, c) -> e.apply(s, c, sink);
        }
        OrderBook b = new OrderBook(setup.book);
        return (s, c) -> b.apply(c, sink);
    }

    static void applyAll(Target t, Corpus c, int n) {
        for (int i = 0; i < n; i++) t.apply(c.syms[i], c.cmds[i]);
    }

    static long threadAlloc() {
        var mx = ManagementFactory.getThreadMXBean();
        if (mx instanceof com.sun.management.ThreadMXBean sun) return sun.getThreadAllocatedBytes(Thread.currentThread().getId());
        return -1;
    }

    static Row coreMode(Corpus setup, Corpus run, int warmups) {
        Row r = new Row();
        r.ops = run.cmds.length;
        Sink.Null sink = new Sink.Null();
        for (int w = 0; w < warmups; w++) {  // spec warmup (setup + 10% of run), repeated for the JIT
            Target t = make(setup, sink);
            applyAll(t, setup, setup.cmds.length);
            applyAll(t, run, w == warmups - 1 ? run.cmds.length / 10 : run.cmds.length);
        }
        {  // timed per op (matcher protocol)
            Target t = make(setup, sink);
            applyAll(t, setup, setup.cmds.length);
            System.gc();
            r.lat = new long[run.cmds.length];
            long wall = System.nanoTime();
            for (int i = 0; i < run.cmds.length; i++) {
                long t0 = System.nanoTime();
                t.apply(run.syms[i], run.cmds[i]);
                r.lat[i] = System.nanoTime() - t0;
            }
            r.wallNs = System.nanoTime() - wall;
        }
        {  // untimed: the scaling gate's denominator (spec/BENCH.md 1.1)
            Target t = make(setup, sink);
            applyAll(t, setup, setup.cmds.length);
            System.gc();
            long a0 = threadAlloc();
            long wall = System.nanoTime();
            applyAll(t, run, run.cmds.length);
            long ns = System.nanoTime() - wall;
            long a1 = threadAlloc();
            r.untimed = run.cmds.length / (ns / 1e9);
            if (a0 >= 0) r.allocPerOp = (double) (a1 - a0) / run.cmds.length;
        }
        if (sink.acc == 42) System.err.print("");
        return r;
    }

    static final class PipeOpts {
        int partitions = 1, producers = 1, batch = 64;
        Journal.Config2 journal;
        Pipeline.Waits waits = Pipeline.Waits.lowLatency();
        int ingress = 1 << 14, inbox = 1 << 12, outbox = 1 << 13;
    }

    static <C extends Core.MatchingCore> Pipeline<C> build(Core.Factory<C> f, OrderBook.Config book, PipeOpts o,
                                                           Egress.Factory m) {
        var b = Pipeline.builder(f).bookConfig(book).partitions(o.partitions).waits(o.waits)
                .ringSizes(o.ingress, o.inbox, o.outbox);
        if (o.journal != null) b.journal(o.journal);
        if (m != null) b.egress(m);
        return b.build();
    }

    static <C extends Core.MatchingCore> Row pipeMode(Core.Factory<C> f, Corpus setup, Corpus run, PipeOpts o,
                                                      int warmups) throws Exception {
        for (int w = 0; w < warmups; w++) {  // warmup on throwaway pipelines
            try (Pipeline<C> p = build(f, setup.book, o, null)) {
                p.publishBatch(setup.syms, setup.cmds);
                int n = w == warmups - 1 ? run.cmds.length / 10 : run.cmds.length;
                p.handle().publishBatch(run.syms, run.cmds, 0, n);
                p.drain();
            }
        }
        Egress.Metrics m = Egress.metrics(run.cmds.length + 1024);
        Pipeline<C> p = build(f, setup.book, o, m.factory());
        p.publishBatch(setup.syms, setup.cmds);
        p.drain();
        System.gc();  // the corpus is live for the whole run: start the timed pass from a collected heap
        List<long[]> ssyms = new ArrayList<>();
        List<Command[]> scmds = new ArrayList<>();
        int[] counts = new int[o.producers];
        for (long s : run.syms) counts[(int) Long.remainderUnsigned(s, o.producers)]++;
        for (int i = 0; i < o.producers; i++) { ssyms.add(new long[counts[i]]); scmds.add(new Command[counts[i]]); }
        int[] fill = new int[o.producers];
        for (int i = 0; i < run.cmds.length; i++) {
            int k = (int) Long.remainderUnsigned(run.syms[i], o.producers);
            ssyms.get(k)[fill[k]] = run.syms[i];
            scmds.get(k)[fill[k]++] = run.cmds[i];
        }
        p.setTimestamps(true);
        CyclicBarrier start = new CyclicBarrier(o.producers + 1);
        List<Thread> ts = new ArrayList<>();
        for (int k = 0; k < o.producers; k++) {
            long[] sy = ssyms.get(k);
            Command[] cm = scmds.get(k);
            Pipeline.Handle h = p.handle();
            Thread t = new Thread(() -> {
                try { start.await(); } catch (Exception e) { throw new RuntimeException(e); }
                for (int i = 0; i < cm.length; i += o.batch) h.publishBatch(sy, cm, i, Math.min(o.batch, cm.length - i));
                h.close();
            });
            t.start();
            ts.add(t);
        }
        start.await();
        long wall = System.nanoTime();
        for (Thread t : ts) t.join();
        p.drain();
        Row r = new Row();
        r.wallNs = System.nanoTime() - wall;
        r.ops = run.cmds.length;
        p.setTimestamps(false);
        p.shutdown();
        int total = 0;
        for (Egress.PartitionMetrics pm : m.handle().results()) total += pm.samples;
        r.lat = new long[total];
        int at = 0;
        for (Egress.PartitionMetrics pm : m.handle().results()) {
            System.arraycopy(pm.latencies, 0, r.lat, at, pm.samples);
            at += pm.samples;
        }
        return r;
    }

    static long pct(long[] v, double p) {
        if (v.length == 0) return 0;
        int i = (int) Math.ceil((v.length - 1) * p);
        return v[Math.min(i, v.length - 1)];
    }

    static String cpu() {
        try {
            Process pr = new ProcessBuilder("sysctl", "-n", "machdep.cpu.brand_string").redirectErrorStream(true).start();
            String s = new String(pr.getInputStream().readAllBytes()).trim();
            if (pr.waitFor() == 0 && !s.isEmpty()) return s;
        } catch (Exception ignored) {}
        return "unknown cpu";
    }

    static void deleteTree(Path p) {
        try (var w = Files.walk(p)) {
            w.sorted(Comparator.reverseOrder()).forEach(q -> { try { Files.deleteIfExists(q); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
    }

    public static void main(String[] argv) throws Exception {
        String usage = "orderbench <prefix> --mode core|pipe [--partitions P] [--producers N] [--journal binary|jsonl|off] "
                + "[--journal-dir DIR] [--fsync N] [--tag NAME] [--core fifo|noop] [--waits relaxed|low] [--batch N] "
                + "[--ingress N] [--inbox N] [--outbox N] [--events on|off] [--baseline OPS] [--warmups N]";
        Args a = new Args(argv, usage, List.of("--mode", "--partitions", "--producers", "--journal", "--journal-dir",
                "--fsync", "--tag", "--core", "--waits", "--batch", "--ingress", "--inbox", "--outbox", "--events",
                "--baseline", "--warmups"), List.of());
        if (a.positional.size() != 1) throw Harness.die(usage);
        String prefix = a.positional.get(0);
        String tag = a.get("--tag") != null ? a.get("--tag") : prefix.substring(prefix.lastIndexOf('/') + 1);
        Corpus setup = Harness.loadCorpus(prefix + ".setup.cmd.jsonl");
        Corpus run = Harness.loadCorpus(prefix + ".run.cmd.jsonl");
        String mode = a.get("--mode") != null ? a.get("--mode") : "core";
        int warmups = (int) Math.max(a.num("--warmups", 3, 100), 1);
        List<String> config = new ArrayList<>();
        Row row;
        String P = "-", prod = "-";
        if (mode.equals("core")) {
            row = coreMode(setup, run, warmups);
        } else if (mode.equals("pipe")) {
            PipeOpts o = new PipeOpts();
            o.partitions = (int) a.num("--partitions", 1, 1024);
            o.producers = (int) Math.max(a.num("--producers", 1, 1024), 1);
            o.batch = (int) Math.max(a.num("--batch", 64, 1 << 20), 1);
            o.ingress = (int) a.num("--ingress", o.ingress, 1 << 30);
            o.inbox = (int) a.num("--inbox", o.inbox, 1 << 30);
            o.outbox = (int) a.num("--outbox", o.outbox, 1 << 30);
            long fsync = a.num("--fsync", 1024, Long.MAX_VALUE);
            String jm = a.get("--journal") != null ? a.get("--journal") : "binary";
            Path tmp = Path.of(System.getProperty("java.io.tmpdir"), "orderbench-java-" + ProcessHandle.current().pid());
            if (jm.equals("binary") || jm.equals("jsonl")) {
                Path dir = a.get("--journal-dir") != null ? Path.of(a.get("--journal-dir")) : tmp;
                o.journal = Journal.Config2.of(dir, jm.equals("binary") ? Journal.Format.Binary : Journal.Format.Jsonl)
                        .withFsync(fsync > 0 ? Journal.FsyncPolicy.everyN(fsync) : Journal.FsyncPolicy.never())
                        .withEvents("on".equals(a.get("--events")));
            } else if (!jm.equals("off")) {
                throw Harness.die("--journal: unknown mode " + jm);
            }
            String w = a.get("--waits") != null ? a.get("--waits") : "low";
            if (w.equals("relaxed")) o.waits = Pipeline.Waits.relaxed();
            else if (!w.equals("low")) throw Harness.die("--waits: unknown " + w);
            config.add("journal=" + jm + " fsync=" + fsync);
            for (String k : List.of("--core", "--waits", "--batch", "--ingress", "--inbox", "--outbox", "--events", "--warmups"))
                if (a.get(k) != null) config.add(k.substring(2) + "=" + a.get(k));
            String core = a.get("--core") != null ? a.get("--core") : "fifo";
            try {
                if (core.equals("fifo")) row = pipeMode(Core.FifoCore::new, setup, run, o, warmups);
                else if (core.equals("noop")) row = pipeMode(Core.NoopCore::new, setup, run, o, warmups);
                else throw Harness.die("--core: unknown " + core);
            } catch (Pipeline.PipelineError e) {
                throw Harness.fail(e.getMessage());
            }
            deleteTree(tmp);
            P = Integer.toString(o.partitions);
            prod = Integer.toString(o.producers);
        } else {
            throw Harness.die("--mode: unknown " + mode);
        }
        if (row.untimed != null) config.add("untimed=" + (long) (double) row.untimed);
        if (row.allocPerOp != null) config.add(String.format("alloc_b_op=%.0f", row.allocPerOp));
        Arrays.sort(row.lat);
        double opsS = row.ops / (row.wallNs / 1e9);
        long mean = 0;
        if (row.lat.length > 0) {
            double sum = 0;
            for (long v : row.lat) sum += v;
            mean = (long) (sum / row.lat.length);
        }
        String eff = "";
        if (mode.equals("pipe") && a.get("--baseline") != null)
            eff = String.format("%.2f", opsS / (Double.parseDouble(P) * Double.parseDouble(a.get("--baseline"))));
        System.out.printf("| %s | %s | %s | %s | %d | %.0f | %s | %d | %d | %d | %d | %d | %d | %s |%n", tag, mode, P, prod,
                row.ops, opsS, eff, mean, pct(row.lat, 0.5), pct(row.lat, 0.9), pct(row.lat, 0.99), pct(row.lat, 0.999),
                row.lat.length == 0 ? 0 : row.lat[row.lat.length - 1], String.join(" ", config));
        System.out.flush();
        System.err.println("env: " + cpu() + " / orderer-java 0.2.1 / java " + System.getProperty("java.version"));
        System.exit(0);
    }
}
