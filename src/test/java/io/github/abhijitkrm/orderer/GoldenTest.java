// A1 — every vendored matcher vector (and orderer's regress vectors), every
// index mode, through the live pipeline: P=1 byte-identical to .evt; P=4
// per-symbol identical (engine) / unchanged (single-book), routing honored.
//   GoldenTest [vectors-dir]
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.PriceIndex;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class GoldenTest {
    static Path VEC = Path.of("vectors");

    static int runVector(Path cmd, Path evt) {
        String text = T.slurp(cmd);
        Harness.Corpus c = Harness.parseCorpus(text, cmd.toString());
        List<String> expected = T.lines(T.slurp(evt));
        expected.remove(0);
        String ix = Flat.get(text.substring(0, text.indexOf('\n')), "index");
        List<PriceIndex.Kind> modes = List.of(PriceIndex.Kind.Ladder);
        if ("both".equals(ix)) modes = List.of(PriceIndex.Kind.Ladder, PriceIndex.Kind.Tree);
        if ("tree".equals(ix)) modes = List.of(PriceIndex.Kind.Tree);
        int runs = 0;
        for (PriceIndex.Kind kind : modes) {
            Config cfg = new Config(c.book.priceMin(), c.book.priceMax(), c.book.maxOrders(), kind);
            check(T.concat(T.runPipeline(Core.FifoCore::new, cfg, c.syms, c.cmds, 1, c.engine)).equals(expected), cmd, " P=1");
            List<List<String>> parts = T.runPipeline(Core.FifoCore::new, cfg, c.syms, c.cmds, 4, c.engine);
            if (c.engine) {
                check(T.bySymbol(T.concat(parts)).equals(T.bySymbol(expected)), cmd, " P=4");
                for (int p = 0; p < parts.size(); p++)
                    for (String l : parts.get(p))
                        check(Routing.hashPartition(Flat.u64(l, "symbol"), 4) == p, "routing ", l);
            } else {
                check(T.concat(parts).equals(expected), cmd, " P=4 single-book");
            }
            runs++;
        }
        return runs;
    }

    static void everyVectorThroughPipeline() throws Exception {
        int runs = 0;
        String mf = T.slurp(VEC.resolve("matcher/manifest.json"));
        for (int pos = 0; (pos = mf.indexOf("\"file\"", pos)) >= 0;) {
            int a = mf.indexOf('"', mf.indexOf(':', pos) + 1) + 1, b = mf.indexOf('"', a);
            String name = mf.substring(a, b);
            pos = b;
            runs += runVector(VEC.resolve("matcher/" + name + ".cmd.jsonl"), VEC.resolve("matcher/" + name + ".evt.jsonl"));
        }
        try (var ds = Files.list(VEC.resolve("regress"))) {
            for (Path p : (Iterable<Path>) ds::iterator) {
                String s = p.toString();
                if (s.endsWith(".cmd.jsonl")) runs += runVector(p, Path.of(s.substring(0, s.length() - 10) + ".evt.jsonl"));
            }
        }
        check(runs >= 80, "only ", runs, " vector runs");
        System.out.println("     " + runs + " vector runs");
    }

    static void routingVectors() {
        int n = 0;
        for (String l : T.lines(T.slurp(VEC.resolve("routing/hash.jsonl")))) {
            if (l.contains("\"format\"")) continue;
            long s = Flat.u64(l, "symbol");
            int P = (int) (long) Flat.u64(l, "partitions"), want = (int) (long) Flat.u64(l, "partition");
            check(Routing.hashPartition(s, P) == want && new PartitionMap(P).partition(s) == want, l);
            n++;
        }
        check(n > 500);
        PartitionMap m = PartitionMap.parseTable(T.slurp(VEC.resolve("routing/table.map.jsonl")), 4);
        for (String l : T.lines(T.slurp(VEC.resolve("routing/table.expect.jsonl")))) {
            if (l.contains("\"format\"")) continue;
            check(m.partition(Flat.u64(l, "symbol")) == Flat.u64(l, "partition"), l);
        }
        boolean threw = false;
        try { PartitionMap.parseTable(T.slurp(VEC.resolve("routing/table.map.jsonl")), 3); }
        catch (Routing.RoutingError e) { threw = true; }
        check(threw, "P mismatch must fail");
    }

    public static void main(String[] args) {
        if (args.length > 0) VEC = Path.of(args[0]);
        T.runAll(new Object[][] {
            {"every_vector_through_pipeline", (T.Body) GoldenTest::everyVectorThroughPipeline},
            {"routing_vectors", (T.Body) GoldenTest::routingVectors},
        });
    }
}
