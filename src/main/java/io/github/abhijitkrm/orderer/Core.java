//! The MatchingCore seam (orderer-rust orderer-core/src/core.rs): what a
//! partition's engine thread needs from a matching core — plus strict
//! snapshot parsing and restore validation.
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.OrderBook.RestingOrder;
import io.github.abhijitkrm.matcher.PriceIndex;
import io.github.abhijitkrm.matcher.Sink;
import io.github.abhijitkrm.matcher.Snapshot;
import io.github.abhijitkrm.matcher.Types.CloseReason;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class Core {
    private Core() {}

    /// emit(symbol, seq, event) for every event, in match order.
    @FunctionalInterface
    public interface Emit { void onEvent(long sym, long seq, Event ev); }

    /// One snapshot book block (matcher-snap/1 text, no header).
    public record Block(long symbol, String text) {}

    public interface MatchingCore {
        void apply(long sym, Command cmd, Emit emit);
        /// This core's book blocks, any order (the pipeline merges by symbol).
        void snapshotBlocks(List<Block> out);
        /// Install one book from a snapshot block; error text, or null.
        String restoreBook(long sym, long seq, List<RestingOrder> orders);
    }

    @FunctionalInterface
    public interface Factory<C extends MatchingCore> { C create(Config cfg); }

    /// Can `orders` be restored as `sym`'s book under `cfg`?
    public static String validateBook(Config cfg, long sym, List<RestingOrder> orders) {
        String pre = "snapshot book " + sym + ": ";
        if (orders.size() > cfg.maxOrders()) return pre + orders.size() + " orders exceed max_orders " + cfg.maxOrders();
        long[] ids = new long[orders.size()];
        for (int i = 0; i < ids.length; i++) ids[i] = orders.get(i).orderId();
        Arrays.sort(ids);
        for (int i = 1; i < ids.length; i++) if (ids[i] == ids[i - 1]) return pre + "duplicate order_id";
        boolean haveBid = false, haveAsk = false;
        long bestBid = 0, bestAsk = 0;
        for (RestingOrder o : orders) {
            if (o.qty() == 0) return pre + "order " + o.orderId() + " has qty 0";
            boolean ok = cfg.index() == PriceIndex.Kind.Ladder
                    ? o.price() >= cfg.priceMin() && o.price() <= cfg.priceMax() : o.price() > 0;
            if (!ok) return pre + "order " + o.orderId() + " price out of range";
            if (o.side() == Side.Bid) { bestBid = haveBid ? Math.max(bestBid, o.price()) : o.price(); haveBid = true; }
            else { bestAsk = haveAsk ? Math.min(bestAsk, o.price()) : o.price(); haveAsk = true; }
        }
        if (haveBid && haveAsk && bestBid >= bestAsk) return pre + "crossed book";
        return null;
    }

    /// The spec-proven FIFO core: matcher-java's OrderBook, one per symbol,
    /// with a primitive-keyed index (no Long boxing per command) and a reused
    /// tagging sink (no lambda per command).
    public static final class FifoCore implements MatchingCore {
        private final Config cfg;
        private final OrderBook[] dense = new OrderBook[4096];
        private final Map<Long, OrderBook> sparse = new HashMap<>();
        private final TagSink tag = new TagSink();

        public FifoCore(Config cfg) { this.cfg = cfg; }

        private static final class TagSink implements Sink {
            long sym;
            Emit emit;
            public void onEvent(long seq, Event ev) { emit.onEvent(sym, seq, ev); }
        }

        OrderBook book(long sym) {
            if (sym >= 0 && sym < dense.length) {
                OrderBook b = dense[(int) sym];
                if (b == null) dense[(int) sym] = b = new OrderBook(cfg);
                return b;
            }
            return sparse.computeIfAbsent(sym, k -> new OrderBook(cfg));
        }

        public void apply(long sym, Command cmd, Emit emit) {
            tag.sym = sym;
            tag.emit = emit;
            book(sym).apply(cmd, tag);
        }

        public void snapshotBlocks(List<Block> out) {
            for (int s = 0; s < dense.length; s++)
                if (dense[s] != null) out.add(block(s, dense[s]));
            for (var e : sparse.entrySet()) out.add(block(e.getKey(), e.getValue()));
        }

        private static Block block(long sym, OrderBook b) {
            StringBuilder sb = new StringBuilder();
            Snapshot.writeBook(b, sym, sb);
            return new Block(sym, sb.toString());
        }

        public String restoreBook(long sym, long seq, List<RestingOrder> orders) {
            String err = validateBook(cfg, sym, orders);
            if (err != null) return err;
            OrderBook b = OrderBook.restore(cfg, seq, orders);
            if (sym >= 0 && sym < dense.length) dense[(int) sym] = b; else sparse.put(sym, b);
            return null;
        }
    }

    /// Test core: echoes each command as one event with a dense per-symbol seq.
    public static final class NoopCore implements MatchingCore {
        private final TreeMap<Long, long[]> seqs = new TreeMap<>();
        public NoopCore(Config cfg) {}

        public void apply(long sym, Command cmd, Emit emit) {
            long[] s = seqs.computeIfAbsent(sym, k -> new long[1]);
            s[0]++;
            Event ev;
            if (cmd instanceof Command.New n) ev = new Event.Accepted(n.orderId(), n.qty());
            else if (cmd instanceof Command.Cancel x) ev = new Event.Closed(x.orderId(), CloseReason.Cancelled);
            else { Command.Replace r = (Command.Replace) cmd; ev = new Event.Replaced(r.orderId(), r.price(), r.qty()); }
            emit.onEvent(sym, s[0], ev);
        }
        public void snapshotBlocks(List<Block> out) {
            for (var e : seqs.entrySet())
                out.add(new Block(e.getKey(), "{\"rec\":\"book\",\"symbol\":" + e.getKey() + ",\"seq\":" + e.getValue()[0] + "}\n"));
        }
        public String restoreBook(long sym, long seq, List<RestingOrder> orders) {
            seqs.put(sym, new long[] {seq});
            return null;
        }
    }

    // ---- matcher-snap/1, strictly -----------------------------------------------------------

    public record SnapBook(long symbol, long seq, List<RestingOrder> orders) {}
    public record ParsedSnapshot(Config cfg, List<SnapBook> books) {}

    public static String header(Config c) {
        return "{\"format\":\"matcher-snap/1\",\"pmin\":" + c.priceMin() + ",\"pmax\":" + c.priceMax()
                + ",\"max_orders\":" + c.maxOrders() + ",\"index\":\"" + Flat.indexName(c.index()) + "\"}\n";
    }

    public static final class RestoreError extends RuntimeException {
        public RestoreError(String m) { super(m); }
    }

    /// Strict parse (orderer-rust try_parse): malformed input is an error.
    public static ParsedSnapshot parseSnapshot(String text) {
        String[] lines = text.split("\n", -1);
        int n = lines.length;
        if (n > 0 && lines[n - 1].isEmpty()) n--;  // trailing newline
        if (n == 0 || text.isEmpty()) throw new RestoreError("snapshot line 1: empty snapshot");
        if (!"matcher-snap/1".equals(Flat.get(lines[0], "format")))
            throw new RestoreError("snapshot line 1: not a matcher-snap/1 header");
        Config cfg = Flat.parseHeader(lines[0]);
        List<SnapBook> books = new ArrayList<>();
        for (int i = 1; i < n; i++) {
            String line = lines[i];
            if (line.isEmpty()) continue;
            String rec = Flat.get(line, "rec");
            String err = "snapshot line " + (i + 1) + ": ";
            if ("book".equals(rec)) {
                Long sym = Flat.u64(line, "symbol"), seq = Flat.u64(line, "seq");
                books.add(new SnapBook(sym == null ? 0 : sym & 0xFFFFFFFFL, seq == null ? 0 : seq, new ArrayList<>()));
            } else if ("order".equals(rec)) {
                Long id = Flat.u64(line, "order_id"), price = Flat.i64(line, "price"), qty = Flat.u64(line, "qty");
                String side = Flat.get(line, "side"), tif = Flat.get(line, "tif");
                if (id == null) throw new RestoreError(err + "bad order_id");
                if (!"bid".equals(side) && !"ask".equals(side)) throw new RestoreError(err + "bad side");
                Tif t;
                if ("gtc".equals(tif)) t = Tif.Gtc;
                else if ("ioc".equals(tif)) t = Tif.Ioc;
                else if ("fok".equals(tif)) t = Tif.Fok;
                else if ("post_only".equals(tif)) t = Tif.PostOnly;
                else throw new RestoreError(err + "bad tif");
                if (price == null) throw new RestoreError(err + "bad price");
                if (qty == null) throw new RestoreError(err + "bad qty");
                if (books.isEmpty()) throw new RestoreError(err + "order line before book block");
                books.get(books.size() - 1).orders()
                        .add(new RestingOrder(id, "ask".equals(side) ? Side.Ask : Side.Bid, price, qty, t));
            } else {
                throw new RestoreError(err + "bad rec");
            }
        }
        return new ParsedSnapshot(cfg, books);
    }
}
