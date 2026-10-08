//! Strict flat-JSON field access and canonical command lines.
//!
//! matcher-java's JsonFlat is lenient; orderer's harnesses must reject
//! malformed input exactly as orderer-rust does (spec/HARNESS.md §5), so
//! these mirror matcher-rust's jsonflat: a missing or unparsable field is an
//! error (null).
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.matcher.PriceIndex;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.OType;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;

public final class Flat {
    private Flat() {}

    /// Value of `key`: the quoted string's contents, or the trimmed token; null if absent.
    public static String get(String line, String key) {
        String pat = "\"" + key + "\":";
        int p = line.indexOf(pat);
        if (p < 0) return null;
        int start = p + pat.length();
        if (start < line.length() && line.charAt(start) == '"') {
            int end = line.indexOf('"', start + 1);
            return end < 0 ? null : line.substring(start + 1, end);
        }
        int end = line.length();
        for (int i = start; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == ',' || c == '}') { end = i; break; }
        }
        return line.substring(start, end).trim();
    }

    /// Unsigned 64-bit parse (optional '+'); null on anything else.
    public static Long parseU64(String s) {
        if (s == null) return null;
        if (s.startsWith("+")) s = s.substring(1);
        if (s.isEmpty()) return null;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) < '0' || s.charAt(i) > '9') return null;
        try { return Long.parseUnsignedLong(s); } catch (NumberFormatException e) { return null; }
    }

    public static Long parseI64(String s) {
        if (s == null || s.isEmpty()) return null;
        int i = (s.charAt(0) == '-' || s.charAt(0) == '+') ? 1 : 0;
        if (i == s.length()) return null;
        for (int j = i; j < s.length(); j++) if (s.charAt(j) < '0' || s.charAt(j) > '9') return null;
        try { return Long.parseLong(s.charAt(0) == '+' ? s.substring(1) : s); } catch (NumberFormatException e) { return null; }
    }

    public static Long u64(String line, String key) { return parseU64(get(line, key)); }
    public static Long i64(String line, String key) { return parseI64(get(line, key)); }

    /// One canonical command line; null if any field is missing or invalid.
    public static Command parseCommand(String line) {
        String cmd = get(line, "cmd");
        if (cmd == null) return null;
        switch (cmd) {
            case "new": {
                String side = get(line, "side"), otype = get(line, "otype"), tif = get(line, "tif");
                Long id = u64(line, "order_id"), price = i64(line, "price"), qty = u64(line, "qty");
                if (side == null || otype == null || tif == null || id == null || price == null || qty == null) return null;
                Side s;
                if (side.equals("bid")) s = Side.Bid; else if (side.equals("ask")) s = Side.Ask; else return null;
                OType o;
                if (otype.equals("limit")) o = OType.Limit; else if (otype.equals("market")) o = OType.Market; else return null;
                Tif t;
                switch (tif) {
                    case "gtc": t = Tif.Gtc; break;
                    case "ioc": t = Tif.Ioc; break;
                    case "fok": t = Tif.Fok; break;
                    case "post_only": t = Tif.PostOnly; break;
                    default: return null;
                }
                return new Command.New(id, s, o, price, qty, t);
            }
            case "cancel": {
                Long id = u64(line, "order_id");
                return id == null ? null : new Command.Cancel(id);
            }
            case "replace": {
                Long id = u64(line, "order_id"), price = i64(line, "price"), qty = u64(line, "qty");
                if (id == null || price == null || qty == null) return null;
                return new Command.Replace(id, price, qty);
            }
            default: return null;
        }
    }

    /// Corpus/vector header → default book config (matcher defaults).
    public static OrderBook.Config parseHeader(String line) {
        Long pmin = i64(line, "pmin"), pmax = i64(line, "pmax"), mo = u64(line, "max_orders");
        String ix = get(line, "index");
        return new OrderBook.Config(pmin == null ? 0 : pmin, pmax == null ? 1_000_000 : pmax,
                mo == null ? 65_536 : (int) (long) mo, "tree".equals(ix) ? PriceIndex.Kind.Tree : PriceIndex.Kind.Ladder);
    }

    public static String indexName(PriceIndex.Kind k) { return k == PriceIndex.Kind.Tree ? "tree" : "ladder"; }

    public static boolean sameBook(OrderBook.Config a, OrderBook.Config b) {
        return a.priceMin() == b.priceMin() && a.priceMax() == b.priceMax() && a.maxOrders() == b.maxOrders()
                && a.index() == b.index();
    }

    static String u(long v) { return Long.toUnsignedString(v); }

    /// matcher's canonical command line; engine form (with "symbol") when sym >= 0.
    public static void writeCommand(Command c, long sym, StringBuilder out) {
        String sf = sym >= 0 ? ",\"symbol\":" + sym : "";
        if (c instanceof Command.New n) {
            out.append("{\"cmd\":\"new\"").append(sf).append(",\"order_id\":").append(u(n.orderId()))
               .append(",\"side\":\"").append(n.side().str()).append("\",\"otype\":\"").append(n.otype().str())
               .append("\",\"price\":").append(n.price()).append(",\"qty\":").append(u(n.qty()))
               .append(",\"tif\":\"").append(n.tif().str()).append("\"}");
        } else if (c instanceof Command.Cancel x) {
            out.append("{\"cmd\":\"cancel\"").append(sf).append(",\"order_id\":").append(u(x.orderId())).append('}');
        } else {
            Command.Replace r = (Command.Replace) c;
            out.append("{\"cmd\":\"replace\"").append(sf).append(",\"order_id\":").append(u(r.orderId()))
               .append(",\"price\":").append(r.price()).append(",\"qty\":").append(u(r.qty())).append('}');
        }
    }

    public static long orderId(Command c) {
        if (c instanceof Command.New n) return n.orderId();
        if (c instanceof Command.Cancel x) return x.orderId();
        return ((Command.Replace) c).orderId();
    }
}
