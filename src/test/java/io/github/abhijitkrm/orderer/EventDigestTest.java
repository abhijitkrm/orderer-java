// The event-chain digest (spec/REPLICATION.md 13.16): conformance vectors shared with the
// other languages, sensitivity, continuity across a snapshot restore, cost, and its documented limit.
//
//   EventDigestTest [vectors-dir]
//
// ORDERER_GEN_DIGEST_VECTORS=DIR writes the vector files instead of checking them.
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.matcher.PriceIndex;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import io.github.abhijitkrm.orderer.cluster.Codec;
import io.github.abhijitkrm.orderer.cluster.PipelineSm;
import io.github.abhijitkrm.orderer.Consensus.Entry;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;

public final class EventDigestTest {
    static Path VEC = Path.of("vectors");

    /// Run the commands through a fresh pipeline; return the chains after a drain.
    static long[] chains(OrderBook.Config book, long[] syms, Command[] cmds, int partitions) {
        EventDigest d = new EventDigest(partitions, null);
        Pipeline<Core.FifoCore> p = Pipeline.builder().bookConfig(book).partitions(partitions).egress(d.factory()).build();
        p.publishBatch(syms, cmds);
        p.drain();
        long[] v = d.values();
        p.shutdown();
        return v;
    }

    /// The replica-level digest: SHA-256 over tag, commands applied, partitions, chains.
    static byte[] combine(long iseq, long[] chains) {
        try {
            ByteBuffer b = ByteBuffer.allocate(18 + 12 + 8 * chains.length).order(ByteOrder.LITTLE_ENDIAN);
            b.put("orderer-evdigest/1".getBytes(StandardCharsets.US_ASCII)).putLong(iseq).putInt(chains.length);
            for (long c : chains) b.putLong(c);
            return MessageDigest.getInstance("SHA-256").digest(b.array());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    static String vectorText(long iseq, long[] chains) {
        StringBuilder cs = new StringBuilder();
        for (long c : chains) cs.append(cs.length() > 0 ? " " : "").append(String.format("%016x", c));
        return "orderer-evdigest/1\npartitions " + chains.length + "\ncommands " + iseq + "\nchains " + cs + "\ndigest "
                + HexFormat.of().formatHex(combine(iseq, chains)) + "\n";
    }

    static PipelineSm<Core.FifoCore> sm(OrderBook.Config book, int partitions) {
        PartitionMap pm = new PartitionMap(partitions);
        return new PipelineSm<>(Core.FifoCore::new, book, pm, (init, digest) -> {
            Pipeline.Builder<Core.FifoCore> b = Pipeline.builder(Core.FifoCore::new).bookConfig(book).partitionMap(pm)
                    .egress(digest.factory()).timestamps(false);
            if (init != null) b.initial(init);
            return b.build();
        });
    }

    static void apply(PipelineSm<?> sm, long index, long[] syms, Command[] cmds) {
        sm.apply(index, new Entry(1, Codec.encodeBatch(syms, cmds, null)));
    }

    public static void main(String[] args) {
        if (args.length > 0) VEC = Path.of(args[0]);
        T.runAll(new Object[][] {
            {"chains_match_the_shared_vectors", (T.Body) () -> {
                String gen = System.getenv("ORDERER_GEN_DIGEST_VECTORS");
                int checked = 0;
                for (String name : new String[] {"multisymbol", "fuzz_s11"}) {
                    Harness.Corpus c = Harness.loadCorpus(VEC.resolve("pipeline").resolve(name + ".cmd.jsonl").toString());
                    for (int p : new int[] {1, 2, 4}) {
                        String text = vectorText(c.cmds.length, chains(c.book, c.syms, c.cmds, p));
                        String file = name + ".P" + p + ".txt";
                        if (gen != null) Files.writeString(Path.of(gen, file), text);
                        else { check(text.equals(Files.readString(VEC.resolve("replication-digest").resolve(file))), file); checked++; }
                    }
                }
                if (gen == null) check(checked == 6, "checked ", checked);
            }},
            {"chains_are_deterministic_and_sensitive_to_every_field", (T.Body) () -> {
                int n = 40 + 20;
                long[] syms = new long[n];
                Command[] cmds = new Command[n];
                for (int i = 1; i <= 40; i++) {
                    syms[i - 1] = i % 5;
                    cmds[i - 1] = Command.newLimit(i, i % 2 == 0 ? Side.Bid : Side.Ask, 100 + i % 3, 1 + i % 4, Tif.Gtc);
                }
                // Sweeping orders at the end trade against whatever rests, which exposes resting prices and quantities.
                int k = 40;
                for (long s : new long[] {0, 1, 2, 3, 4, 7, 8, 9, 10, 11}) {
                    syms[k] = s; cmds[k++] = Command.newLimit(9000 + s, Side.Bid, 1000, 10_000, Tif.Ioc);
                    syms[k] = s; cmds[k++] = Command.newLimit(9100 + s, Side.Ask, 0, 10_000, Tif.Ioc);
                }
                OrderBook.Config book = OrderBook.Config.defaults();
                // A narrower range keeps memory modest for the sweeps' symbols.
                book = new OrderBook.Config(0, 20_000, 65_536, PriceIndex.Kind.Ladder);
                long[] ref = chains(book, syms, cmds, 2);
                check(Arrays.equals(ref, chains(book, syms, cmds, 2)), "not deterministic");
                Command c0 = cmds[0];
                Command.New n0 = (Command.New) c0;
                Command[][] variants = {
                    {new Command.New(n0.orderId(), n0.side(), n0.otype(), n0.price(), n0.qty() + 1, n0.tif())},
                    {new Command.New(n0.orderId(), n0.side(), n0.otype(), n0.price() + 1, n0.qty(), n0.tif())},
                    {new Command.New(n0.orderId() + 1000, n0.side(), n0.otype(), n0.price(), n0.qty(), n0.tif())},
                };
                String[] names = {"qty", "price", "order id"};
                for (int v = 0; v < variants.length; v++) {
                    Command[] edited = cmds.clone();
                    edited[0] = variants[v][0];
                    check(!Arrays.equals(ref, chains(book, syms, edited, 2)), "a different ", names[v], " did not change the chain");
                }
                long[] symEdit = syms.clone();
                symEdit[0] += 7;
                check(!Arrays.equals(ref, chains(book, symEdit, cmds, 2)), "a different symbol did not change the chain");
                check(!Arrays.equals(ref, chains(book, Arrays.copyOf(syms, n - 1), Arrays.copyOf(cmds, n - 1), 2)), "dropping a command did not change the chain");
            }},
            {"a_restored_pipeline_continues_the_same_chain", (T.Body) () -> {
                Harness.Corpus c = Harness.loadCorpus(VEC.resolve("pipeline").resolve("fuzz_s11.cmd.jsonl").toString());
                int p = 2;
                long[] whole = chains(c.book, c.syms, c.cmds, p);
                EventDigest d1 = new EventDigest(p, null);
                Pipeline<Core.FifoCore> first = Pipeline.builder().bookConfig(c.book).partitions(p).egress(d1.factory()).build();
                first.handle().publishBatch(c.syms, c.cmds, 0, 1500);
                first.drain();
                Pipeline.Snapshot snap = first.snapshot();
                long[] mid = d1.values();
                first.shutdown();
                PartitionMap map = new PartitionMap(p);
                Recover.Restored<Core.FifoCore> r = Recover.restore(Core.FifoCore::new, c.book, map, snap);
                EventDigest d2 = new EventDigest(p, mid);
                Pipeline<Core.FifoCore> second = Pipeline.builder().bookConfig(c.book).partitionMap(map).egress(d2.factory())
                        .initial(new Pipeline.Initial<>(r.cores(), snap.iseq() + 1)).build();
                second.handle().publishBatch(c.syms, c.cmds, 1500, c.cmds.length - 1500);
                second.drain();
                check(Arrays.equals(whole, d2.values()), "the chain broke across a snapshot");
                second.shutdown();
            }},
            {"the_digest_costs_a_barrier_not_a_snapshot", (T.Body) () -> {
                OrderBook.Config book = new OrderBook.Config(0, 20_000, 65_536, PriceIndex.Kind.Ladder);
                PipelineSm<Core.FifoCore> sm = sm(book, 2);
                try {
                    int n = 300_000;
                    long[] syms = new long[n];
                    Command[] cmds = new Command[n];
                    for (int i = 1; i <= n; i++) {   // 300k resting orders over 8 symbols that never cross
                        syms[i - 1] = i % 8;
                        cmds[i - 1] = i % 2 == 0 ? Command.newLimit(i, Side.Bid, 1000 - i % 500, 1, Tif.Gtc) : Command.newLimit(i, Side.Ask, 2000 + i % 500, 1, Tif.Gtc);
                    }
                    apply(sm, 1, syms, cmds);
                    sm.digest(1);   // warm up
                    long t = System.nanoTime();
                    byte[] cheap = sm.digest(1);
                    long cheapNs = System.nanoTime() - t;
                    t = System.nanoTime();
                    byte[] deep = sm.deepDigest(1);
                    long deepNs = System.nanoTime() - t;
                    System.out.printf("  digest %.2f ms, deep digest %.1f ms (300k resting orders)%n", cheapNs / 1e6, deepNs / 1e6);
                    check(!Arrays.equals(cheap, deep), "digest and deep digest coincide");
                    check(cheapNs < deepNs / 10, "digest ", cheapNs, " ns is not much cheaper than deep ", deepNs, " ns");
                    check(cheapNs < 100_000_000L, "digest took ", cheapNs, " ns");
                } finally { sm.close(); }
            }},
            {"a_resting_difference_is_invisible_to_the_chain_until_it_trades", (T.Body) () -> {
                // The limit of an event-stream digest, stated as a test: books that differ only in a resting
                // order's price emit the same events until something trades against it. The deep digest sees it at once.
                OrderBook.Config book = new OrderBook.Config(0, 20_000, 65_536, PriceIndex.Kind.Ladder);
                byte[][] out = new byte[4][];
                long[] prices = {100, 101};
                for (int i = 0; i < 2; i++) {
                    PipelineSm<Core.FifoCore> sm = sm(book, 1);
                    try {
                        apply(sm, 1, new long[] {1}, new Command[] {Command.newLimit(1, Side.Ask, prices[i], 5, Tif.Gtc)});
                        out[2 * i] = sm.digest(1);
                        out[2 * i + 1] = sm.deepDigest(1);
                    } finally { sm.close(); }
                }
                check(Arrays.equals(out[0], out[2]), "the chain saw a price that has not traded");
                check(!Arrays.equals(out[1], out[3]), "the deep digest missed a different resting price");
            }},
        });
    }
}
