// Test helpers: a minimal runner, vectors, a seeded adversarial generator,
// single-Engine references, per-symbol views, pipeline runs.
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.Engine;
import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.PriceIndex;
import io.github.abhijitkrm.matcher.Sink;
import io.github.abhijitkrm.matcher.Snapshot;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.matcher.Types.OType;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class T {
    private T() {}

    interface Body { void run() throws Exception; }

    private static int failures, checks;
    private static String current = "";

    static void check(boolean ok, Object... msg) {
        checks++;
        if (!ok) {
            failures++;
            StringBuilder sb = new StringBuilder();
            for (Object m : msg) sb.append(m);
            if (failures <= 20) System.out.println("  FAIL [" + current + "] " + sb + "  at " + Thread.currentThread().getStackTrace()[2]);
        }
    }

    /// Runs named tests; exits nonzero on any failure.
    static void runAll(Object[][] tests) {
        long t0 = System.nanoTime();
        for (Object[] t : tests) {
            current = (String) t[0];
            int before = failures;
            long s = System.nanoTime();
            try { ((Body) t[1]).run(); }
            catch (Throwable e) { failures++; System.out.println("  FAIL [" + current + "] threw " + e); e.printStackTrace(System.out); }
            System.out.printf("%s %s (%d ms)%n", failures == before ? "ok  " : "FAIL", current, (System.nanoTime() - s) / 1_000_000);
        }
        System.out.printf("%d tests, %d checks, %d failures (%d ms)%n", tests.length, checks, failures, (System.nanoTime() - t0) / 1_000_000);
        System.exit(failures == 0 ? 0 : 1);
    }

    static String slurp(Path p) {
        try { return Files.readString(p, StandardCharsets.UTF_8); }
        catch (IOException e) { throw new RuntimeException(e); }
    }

    static List<String> lines(String s) {
        List<String> out = new ArrayList<>();
        int pos = 0;
        while (pos < s.length()) {
            int e = s.indexOf('\n', pos);
            if (e < 0) e = s.length();
            out.add(s.substring(pos, e));
            pos = e + 1;
        }
        return out;
    }

    static final class Cmds {
        final List<Long> syms = new ArrayList<>();
        final List<Command> cmds = new ArrayList<>();
        int size() { return cmds.size(); }
        void add(long s, Command c) { syms.add(s); cmds.add(c); }
        long[] symArray() { long[] a = new long[syms.size()]; for (int i = 0; i < a.length; i++) a[i] = syms.get(i); return a; }
        Command[] cmdArray() { return cmds.toArray(new Command[0]); }
        Cmds slice(int from, int to) {
            Cmds c = new Cmds();
            for (int i = from; i < to; i++) c.add(syms.get(i), cmds.get(i));
            return c;
        }
    }

    /// xorshift64* (the tools' generator family).
    static final class Rng {
        long x;
        Rng(long seed) { x = seed; }
        long next() { x ^= x >>> 12; x ^= x << 25; x ^= x >>> 27; return x * 0x2545F4914F6CDD1DL; }
        long below(long n) { return Long.remainderUnsigned(next(), n); }
    }

    static Config fuzzCfg() { return new Config(1, 200, 4096, PriceIndex.Kind.Ladder); }

    /// Adversarial engine stream: crossing prices, small id space, every TIF,
    /// markets, ~5% malformed.
    static Cmds fuzzCorpus(long seed, int n, int symbols) {
        Rng r = new Rng((seed * 0x9E3779B97F4A7C15L) | 1);
        Cmds out = new Cmds();
        Tif[] tifs = {Tif.Gtc, Tif.Gtc, Tif.Ioc, Tif.Fok, Tif.PostOnly};
        long[] bad = {0, 201, -5};
        for (int i = 0; i < n; i++) {
            long sym = r.below(symbols), id = r.below(256);
            long price = r.below(20) == 0 ? bad[(int) r.below(3)] : 90 + r.below(21);
            long qty = r.below(25) == 0 ? 0 : r.below(50) + 1;
            Command c;
            int k = (int) r.below(10);
            if (k < 5) {
                Side side = r.below(2) == 1 ? Side.Ask : Side.Bid;
                if (r.below(8) == 0) c = new Command.New(id, side, OType.Market, 0, qty, Tif.Ioc);
                else c = new Command.New(id, side, OType.Limit, price, qty, tifs[(int) r.below(5)]);
            } else if (k < 8) {
                c = new Command.Cancel(id);
            } else {
                c = new Command.Replace(id, price, qty);
            }
            out.add(sym, c);
        }
        return out;
    }

    static List<String> referenceLines(Config cfg, Cmds cmds) {
        Engine eng = new Engine(cfg);
        List<String> out = new ArrayList<>();
        for (int i = 0; i < cmds.size(); i++)
            eng.applyTagged(cmds.syms.get(i), cmds.cmds.get(i), (s, seq, ev) -> out.add(Event.canonical(seq, s, ev)));
        return out;
    }

    static String referenceSnapshot(Config cfg, Cmds cmds, long n) {
        Engine eng = new Engine(cfg);
        Sink.Null sink = new Sink.Null();
        for (int i = 0; i < n; i++) eng.apply(cmds.syms.get(i), cmds.cmds.get(i), sink);
        return Snapshot.writeEngine(eng);
    }

    static Map<Long, List<String>> bySymbol(List<String> ls) {
        Map<Long, List<String>> m = new TreeMap<>();
        for (String l : ls) {
            Long s = Flat.u64(l, "symbol");
            m.computeIfAbsent(s == null ? 0 : s, k -> new ArrayList<>()).add(l);
        }
        return m;
    }

    static boolean dense(List<String> ls) {
        Map<Long, Long> next = new TreeMap<>();
        for (String l : ls) {
            Long s = Flat.u64(l, "symbol");
            long sym = s == null ? 0 : s;
            long want = next.merge(sym, 1L, Long::sum);
            if (Flat.u64(l, "seq") != want) return false;
        }
        return true;
    }

    /// Per-partition canonical lines from a pipeline run.
    static <C extends Core.MatchingCore> List<List<String>> runPipeline(Core.Factory<C> f, Config cfg, long[] syms,
                                                                         Command[] cmds, int P, boolean tagged) {
        Egress.Collect col = Egress.collect(tagged);
        try (Pipeline<C> p = Pipeline.builder(f).bookConfig(cfg).partitions(P).ringSizes(1 << 10, 1 << 8, 1 << 8)
                .egress(col.factory()).build()) {
            p.publishBatch(syms, cmds);
            p.drain();
            p.shutdown();
        }
        List<List<String>> out = new ArrayList<>();
        for (String b : col.handle().take()) out.add(lines(b));
        return out;
    }

    static List<List<String>> runPipeline(Config cfg, Cmds c, int P) {
        return runPipeline(Core.FifoCore::new, cfg, c.symArray(), c.cmdArray(), P, true);
    }

    static List<String> concat(List<List<String>> v) {
        List<String> out = new ArrayList<>();
        for (List<String> p : v) out.addAll(p);
        return out;
    }

    static Path scratch(Path root, String name) {
        Path d = root.resolve(name);
        try {
            if (Files.exists(d))
                try (var w = Files.walk(d)) { w.sorted(Comparator.reverseOrder()).forEach(q -> q.toFile().delete()); }
            Files.createDirectories(d);
        } catch (IOException e) { throw new RuntimeException(e); }
        return d;
    }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
