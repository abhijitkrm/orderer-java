//! The matching core. Single-writer, deterministic: commands in through
//! `apply`, sequenced events out through the Sink seam. Mirrors spec/SPEC.md.
package io.github.abhijitkrm.matcher;

import io.github.abhijitkrm.matcher.PriceIndex.Level;
import io.github.abhijitkrm.matcher.Types.*;

public final class OrderBook {

    public record Config(long priceMin, long priceMax, int maxOrders, PriceIndex.Kind index) {
        public static Config defaults() {
            return new Config(0, 1_000_000, 65_536, PriceIndex.Kind.Ladder);
        }
    }

    public record OrderInfo(long id, Side side, long price, long qty) {}

    private final Pool pool;
    private final OrderMap map;
    private final PriceIndex bids, asks;
    private long seq = 0;
    private final Config cfg;

    public OrderBook(Config cfg) {
        if (!(cfg.priceMax() > cfg.priceMin()) && cfg.index() != PriceIndex.Kind.Tree) {
            throw new IllegalArgumentException("price range must be non-empty");
        }
        pool = new Pool(cfg.maxOrders());
        map = new OrderMap(cfg.maxOrders());
        bids = cfg.index() == PriceIndex.Kind.Ladder
                ? PriceIndex.ladder(Side.Bid, cfg.priceMin(), cfg.priceMax())
                : PriceIndex.tree(Side.Bid);
        asks = cfg.index() == PriceIndex.Kind.Ladder
                ? PriceIndex.ladder(Side.Ask, cfg.priceMin(), cfg.priceMax())
                : PriceIndex.tree(Side.Ask);
        this.cfg = cfg;
    }

    /// Apply one command, emitting its event stream through `sink`.
    public void apply(Command cmd, Sink sink) {
        if (cmd instanceof Command.New c) {
            newOrder(c.orderId(), c.side(), c.otype(), c.price(), c.qty(), c.tif(), sink);
        } else if (cmd instanceof Command.Cancel c) {
            cancel(c.orderId(), sink);
        } else if (cmd instanceof Command.Replace c) {
            replace(c.orderId(), c.price(), c.qty(), sink);
        }
    }

    // ---- commands ----

    private void newOrder(long orderId, Side side, OType otype, long price, long qty,
                          Tif tif, Sink sink) {
        // SPEC §4.3 validation precedence.
        if (qty == 0) {
            reject(sink, orderId, RejectReason.InvalidQty); return;
        }
        if (otype == OType.Limit && !priceOk(price)) {
            reject(sink, orderId, RejectReason.InvalidPrice); return;
        }
        if (map.contains(orderId)) {
            reject(sink, orderId, RejectReason.DuplicateOrderId); return;
        }
        if (pool.live >= cfg.maxOrders()) {
            reject(sink, orderId, RejectReason.BookFull); return;
        }
        if (otype == OType.Limit) {
            if (tif == Tif.PostOnly) {
                if (wouldCross(side, price)) {
                    reject(sink, orderId, RejectReason.PostOnlyWouldCross); return;
                }
            } else if (tif == Tif.Fok && fillable(side, price) < qty) {
                reject(sink, orderId, RejectReason.FokCannotFill); return;
            }
        }

        Long bound = otype == OType.Limit ? price : null;
        long remaining = cross(side, bound, orderId, qty, sink);

        if (remaining == 0) {
            emit(sink, new Event.Closed(orderId, CloseReason.Filled));
        } else if (otype == OType.Limit && (tif == Tif.Gtc || tif == Tif.PostOnly)) {
            rest(orderId, side, price, remaining, tif);
            emit(sink, new Event.Accepted(orderId, remaining));
        } else {
            emit(sink, new Event.Closed(orderId, CloseReason.Expired));
        }
    }

    private void cancel(long orderId, Sink sink) {
        int idx = map.get(orderId);
        if (idx < 0) {
            reject(sink, orderId, RejectReason.UnknownOrderId); return;
        }
        long price = pool.price[idx];
        PriceIndex own = ownIndex(idx);
        Level lvl = own.levelMut(price);
        if (lvl != null) pool.levelUnlink(lvl, idx);
        own.unlinkLevel(price);
        map.remove(orderId);
        pool.free(idx);
        emit(sink, new Event.Closed(orderId, CloseReason.Cancelled));
    }

    private void replace(long orderId, long price, long qty, Sink sink) {
        // SPEC §4.3: unknown -> invalid_qty -> invalid_price.
        int idx = map.get(orderId);
        if (idx < 0) {
            reject(sink, orderId, RejectReason.UnknownOrderId); return;
        }
        if (qty == 0) {
            reject(sink, orderId, RejectReason.InvalidQty); return;
        }
        if (!priceOk(price)) {
            reject(sink, orderId, RejectReason.InvalidPrice); return;
        }
        long oldPrice = pool.price[idx], oldQty = pool.qty[idx];
        PriceIndex own = ownIndex(idx);

        if (price == oldPrice && qty <= oldQty) {
            // Quantity decrease (or no-op): keeps time priority.
            Level lvl = own.levelMut(price);
            if (lvl != null) lvl.total -= oldQty - qty;
            pool.qty[idx] = qty;
            emit(sink, new Event.Replaced(orderId, price, qty));
            return;
        }

        // Priority loss: unlink and re-enter the aggressive GTC limit path.
        Level oldLvl = own.levelMut(oldPrice);
        if (oldLvl != null) pool.levelUnlink(oldLvl, idx);
        own.unlinkLevel(oldPrice);
        pool.price[idx] = price;
        pool.qty[idx] = qty;

        long remaining = cross(ownSide(idx), price, orderId, qty, sink);

        if (remaining == 0) {
            map.remove(orderId);
            pool.free(idx);
            emit(sink, new Event.Closed(orderId, CloseReason.Filled));
        } else {
            pool.qty[idx] = remaining;
            Level lvl = own.levelInsert(price);
            lvl.total += remaining;
            pool.levelPush(lvl, idx);
            emit(sink, new Event.Replaced(orderId, price, remaining));
        }
    }

    // ---- matching core ----

    /// Aggressive walk of the opposite side. `bound == null` means market
    /// (match all available depth). Returns what is left of `qty`.
    private long cross(Side side, Long bound, long taker, long qty, Sink sink) {
        Pool pool = this.pool;
        OrderMap map = this.map;
        PriceIndex opp = side == Side.Bid ? asks : bids;

        for (;;) {
            long bp = opp.bestPrice();
            if (bp == Long.MIN_VALUE) break;
            if (bound != null) {
                boolean crosses = side == Side.Bid ? bp <= bound : bp >= bound;
                if (!crosses) break;
            }
            Level lvl = opp.levelMut(bp);
            if (lvl == null) break;
            for (;;) {
                int mi = lvl.head;
                if (mi == Pool.NIL) break;
                long mid = pool.id[mi], mqty = pool.qty[mi];
                long q = Math.min(qty, mqty);
                emit(sink, new Event.Trade(mid, taker, bp, q));
                lvl.total -= q;
                pool.qty[mi] = mqty - q;
                qty -= q;
                if (mqty == q) {
                    pool.levelUnlink(lvl, mi);
                    map.remove(mid);
                    pool.free(mi);
                    emit(sink, new Event.Closed(mid, CloseReason.Filled));
                }
                if (qty == 0) break;
            }
            if (lvl.empty()) opp.unlinkLevel(bp);
            if (qty == 0) break;
        }
        return qty;
    }

    /// Insert a resting order (pool slot guaranteed available by the book-full
    /// check at ingest).
    private void rest(long orderId, Side side, long price, long qty, Tif tif) {
        int idx = pool.alloc();
        pool.set(idx, orderId, side, OType.Limit, tif, price, qty);
        PriceIndex own = side == Side.Bid ? bids : asks;
        Level lvl = own.levelInsert(price);
        lvl.total += qty;
        pool.levelPush(lvl, idx);
        map.insert(orderId, idx);
    }

    // ---- queries ----

    public OrderInfo order(long id) {
        int i = map.get(id);
        if (i < 0) return null;
        return new OrderInfo(pool.id[i], Side.values()[pool.side[i]], pool.price[i], pool.qty[i]);
    }

    public long bestBid() { return bids.bestPrice(); }
    public long bestAsk() { return asks.bestPrice(); }
    public int orderCount() { return pool.live; }
    public long seqNo() { return seq; }
    public int levelCount(Side s) { return (s == Side.Bid ? bids : asks).len(); }
    public java.util.List<PriceIndex.Depth> depth(Side s, int n) {
        return (s == Side.Bid ? bids : asks).depth(n);
    }

    // ---- snapshot surface (spec/JOURNAL.md) ----

    /// One live order, for snapshot serialization.
    public record RestingOrder(long orderId, Side side, long price, long qty, Tif tif) {}

    /// All live orders in book order: bids best→worst then asks best→worst,
    /// FIFO within each level.
    public java.util.List<RestingOrder> restingOrders() {
        var out = new java.util.ArrayList<RestingOrder>(pool.live);
        for (Side s : new Side[]{Side.Bid, Side.Ask}) {
            PriceIndex idx = s == Side.Bid ? bids : asks;
            for (PriceIndex.Depth d : idx.depth(Integer.MAX_VALUE)) {
                Level lvl = idx.levelMut(d.price());
                if (lvl == null) continue;
                for (int i = lvl.head; i != Pool.NIL; i = pool.next[i]) {
                    out.add(new RestingOrder(pool.id[i], Side.values()[pool.side[i]],
                            pool.price[i], pool.qty[i], Tif.values()[pool.tif[i]]));
                }
            }
        }
        return out;
    }

    /// Rebuild a book from a snapshot: same config, explicit seq, resting
    /// orders replayed in snapshot order (bids then asks, FIFO per level).
    public static OrderBook restore(Config cfg, long seq,
                                    java.util.List<RestingOrder> orders) {
        OrderBook b = new OrderBook(cfg);
        b.seq = seq;
        for (RestingOrder o : orders) {
            int idx = b.pool.alloc();
            if (idx == Pool.NIL) break;
            b.pool.set(idx, o.orderId(), o.side(), OType.Limit, o.tif(), o.price(), o.qty());
            Level lvl = (o.side() == Side.Bid ? b.bids : b.asks).levelInsert(o.price());
            lvl.total += o.qty();
            b.pool.levelPush(lvl, idx);
            b.map.insert(o.orderId(), idx);
        }
        return b;
    }

    public Config config() { return cfg; }

    // ---- internals ----

    private PriceIndex ownIndex(int poolIdx) {
        return pool.side[poolIdx] == Side.Bid.ordinal() ? bids : asks;
    }

    private Side ownSide(int poolIdx) {
        return Side.values()[pool.side[poolIdx]];
    }

    private boolean priceOk(long price) {
        return cfg.index() == PriceIndex.Kind.Ladder
                ? price >= cfg.priceMin() && price <= cfg.priceMax()
                : price > 0;
    }

    private boolean wouldCross(Side side, long price) {
        if (side == Side.Bid) {
            long bp = asks.bestPrice();
            return bp != Long.MIN_VALUE && price >= bp;
        }
        long bp = bids.bestPrice();
        return bp != Long.MIN_VALUE && price <= bp;
    }

    /// Quantity available on the opposite side within `price` (FOK pre-check).
    private long fillable(Side side, long price) {
        long lo = cfg.index() == PriceIndex.Kind.Ladder ? cfg.priceMin() : Long.MIN_VALUE + 1;
        long hi = cfg.index() == PriceIndex.Kind.Ladder ? cfg.priceMax() : Long.MAX_VALUE;
        return side == Side.Bid ? asks.sumRange(lo, price) : bids.sumRange(price, hi);
    }

    private void emit(Sink sink, Event ev) {
        seq++;
        sink.onEvent(seq, ev);
    }

    private void reject(Sink sink, long orderId, RejectReason reason) {
        emit(sink, new Event.Rejected(orderId, reason));
    }
}
