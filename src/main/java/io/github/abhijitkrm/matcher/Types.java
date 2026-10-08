//! Core types — canonical serialization follows spec/SPEC.md + spec/SCHEMA.md.
package io.github.abhijitkrm.matcher;

public final class Types {
    private Types() {}

    public enum Side { Bid, Ask;
        public String str() { return this == Bid ? "bid" : "ask"; } }

    public enum OType { Limit, Market;
        public String str() { return this == Limit ? "limit" : "market"; } }

    public enum Tif { Gtc, Ioc, Fok, PostOnly;
        public String str() {
            return switch (this) {
                case Gtc -> "gtc"; case Ioc -> "ioc";
                case Fok -> "fok"; case PostOnly -> "post_only";
            };
        } }

    public enum RejectReason {
        InvalidQty, InvalidPrice, DuplicateOrderId, UnknownOrderId,
        PostOnlyWouldCross, FokCannotFill, BookFull;
        public String str() {
            return switch (this) {
                case InvalidQty -> "invalid_qty";
                case InvalidPrice -> "invalid_price";
                case DuplicateOrderId -> "duplicate_order_id";
                case UnknownOrderId -> "unknown_order_id";
                case PostOnlyWouldCross -> "post_only_would_cross";
                case FokCannotFill -> "fok_cannot_fill";
                case BookFull -> "book_full";
            };
        } }

    public enum CloseReason { Filled, Cancelled, Expired;
        public String str() {
            return switch (this) {
                case Filled -> "filled"; case Cancelled -> "cancelled";
                case Expired -> "expired";
            };
        } }

    /// A command submitted to a book.
    public sealed interface Command {
        record New(long orderId, Side side, OType otype, long price, long qty, Tif tif)
                implements Command {}
        record Cancel(long orderId) implements Command {}
        record Replace(long orderId, long price, long qty) implements Command {}

        static Command newLimit(long id, Side s, long p, long q, Tif t) {
            return new New(id, s, OType.Limit, p, q, t);
        }
        static Command newMarket(long id, Side s, long q) {
            return new New(id, s, OType.Market, 0, q, Tif.Ioc);
        }
        static Command cancel(long id) { return new Cancel(id); }
        static Command replace(long id, long p, long q) { return new Replace(id, p, q); }
    }

    /// An event emitted by a book, paired with a per-book seq at emit time.
    public sealed interface Event {
        record Accepted(long orderId, long leavesQty) implements Event {}
        record Rejected(long orderId, RejectReason reason) implements Event {}
        record Trade(long maker, long taker, long price, long qty) implements Event {}
        record Closed(long orderId, CloseReason reason) implements Event {}
        record Replaced(long orderId, long price, long qty) implements Event {}

        /// Canonical event line (SCHEMA.md) — no trailing newline.
        static String canonical(long seq, Event ev) { return canonical(seq, "", ev); }

        /// Canonical line for `engine:true` vectors: `"symbol":N` after `ev`.
        static String canonical(long seq, long sym, Event ev) {
            return canonical(seq, ",\"symbol\":" + sym, ev);
        }

        private static String canonical(long seq, String symField, Event ev) {
            if (ev instanceof Accepted e)
                return "{\"seq\":" + seq + ",\"ev\":\"accepted\"" + symField + ",\"order_id\":" + e.orderId
                        + ",\"leaves_qty\":" + e.leavesQty + "}";
            if (ev instanceof Rejected e)
                return "{\"seq\":" + seq + ",\"ev\":\"rejected\"" + symField + ",\"order_id\":" + e.orderId
                        + ",\"reason\":\"" + e.reason.str() + "\"}";
            if (ev instanceof Trade e)
                return "{\"seq\":" + seq + ",\"ev\":\"trade\"" + symField + ",\"maker\":" + e.maker
                        + ",\"taker\":" + e.taker + ",\"price\":" + e.price + ",\"qty\":" + e.qty + "}";
            if (ev instanceof Closed e)
                return "{\"seq\":" + seq + ",\"ev\":\"closed\"" + symField + ",\"order_id\":" + e.orderId
                        + ",\"reason\":\"" + e.reason.str() + "\"}";
            Replaced e = (Replaced) ev;
            return "{\"seq\":" + seq + ",\"ev\":\"replaced\"" + symField + ",\"order_id\":" + e.orderId
                    + ",\"price\":" + e.price + ",\"qty\":" + e.qty + "}";
        }

        /// Deterministic fold over event fields — NullSink checksum.
        static long fold(Event ev) {
            final long M = 0x9E3779B1L;
            if (ev instanceof Accepted e) return e.orderId * M + e.leavesQty;
            if (ev instanceof Rejected e) return e.orderId * M + e.reason.ordinal();
            if (ev instanceof Trade e) return (e.maker * M + e.taker) * M + e.price * M + e.qty;
            if (ev instanceof Closed e) return e.orderId * M + e.reason.ordinal();
            Replaced e = (Replaced) ev;
            return e.orderId * M + e.price * M + e.qty;
        }
    }
}
