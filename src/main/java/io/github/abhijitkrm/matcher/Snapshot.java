//! Snapshot serialization per spec/JOURNAL.md — flat {"rec":...} lines:
//!   {"format":"matcher-snap/1","pmin":P,"pmax":M,"max_orders":N,"index":"..."}
//!   {"rec":"book","symbol":S,"seq":N}
//!   {"rec":"order","order_id":I,"side":"...","otype":"limit","tif":"...",
//!    "price":P,"qty":Q}
package io.github.abhijitkrm.matcher;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.OrderBook.RestingOrder;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import java.util.ArrayList;
import java.util.List;

public final class Snapshot {
    private Snapshot() {}

    public static void writeBook(OrderBook b, long sym, StringBuilder out) {
        out.append("{\"rec\":\"book\",\"symbol\":").append(sym)
           .append(",\"seq\":").append(b.seqNo()).append("}\n");
        for (RestingOrder o : b.restingOrders()) {
            out.append("{\"rec\":\"order\",\"order_id\":").append(o.orderId())
               .append(",\"side\":\"").append(o.side().str())
               .append("\",\"otype\":\"limit\",\"tif\":\"").append(o.tif().str())
               .append("\",\"price\":").append(o.price())
               .append(",\"qty\":").append(o.qty()).append("}\n");
        }
    }

    public static String writeEngine(Engine e) {
        Config c = e.config();
        StringBuilder out = new StringBuilder();
        out.append("{\"format\":\"matcher-snap/1\",\"pmin\":").append(c.priceMin())
           .append(",\"pmax\":").append(c.priceMax())
           .append(",\"max_orders\":").append(c.maxOrders())
           .append(",\"index\":\"")
           .append(c.index() == PriceIndex.Kind.Tree ? "tree" : "ladder")
           .append("\"}\n");
        for (long s : e.symbolList()) writeBook(e.peek(s), s, out);
        return out.toString();
    }

    public record SnapBook(long symbol, long seq, List<RestingOrder> orders) {}
    public record Snap(Config cfg, List<SnapBook> books) {}

    /// Parse the flat record format written by writeEngine.
    public static Snap parse(String text) {
        Config cfg = Config.defaults();
        List<SnapBook> books = new ArrayList<>();
        SnapBook cur = null;
        List<RestingOrder> curOrders = null;
        for (String line : text.split("\n")) {
            if (line.isEmpty()) continue;
            if (line.contains("\"rec\":\"order\"")) {
                if (curOrders == null) throw new IllegalStateException("order before book");
                Tif tif = switch (JsonFlat.get(line, "tif")) {
                    case "ioc" -> Tif.Ioc;
                    case "fok" -> Tif.Fok;
                    case "post_only" -> Tif.PostOnly;
                    default -> Tif.Gtc;
                };
                curOrders.add(new RestingOrder(
                        JsonFlat.getLong(line, "order_id"),
                        "ask".equals(JsonFlat.get(line, "side")) ? Side.Ask : Side.Bid,
                        JsonFlat.getLong(line, "price"),
                        JsonFlat.getLong(line, "qty"),
                        tif));
            } else if (line.contains("\"rec\":\"book\"")) {
                curOrders = new ArrayList<>();
                cur = new SnapBook(JsonFlat.getLong(line, "symbol"),
                                   JsonFlat.getLong(line, "seq"), curOrders);
                books.add(cur);
            } else {
                cfg = new Config(
                        JsonFlat.getLong(line, "pmin"),
                        JsonFlat.getLong(line, "pmax"),
                        JsonFlat.getLong(line, "max_orders").intValue(),
                        "tree".equals(JsonFlat.get(line, "index"))
                                ? PriceIndex.Kind.Tree : PriceIndex.Kind.Ladder);
            }
        }
        return new Snap(cfg, books);
    }

    /// Rebuild an engine from a parsed snapshot.
    public static Engine restoreEngine(Snap s) {
        Engine e = new Engine(s.cfg());
        for (SnapBook b : s.books())
            e.addBook(b.symbol(), OrderBook.restore(s.cfg(), b.seq(), b.orders()));
        return e;
    }
}
