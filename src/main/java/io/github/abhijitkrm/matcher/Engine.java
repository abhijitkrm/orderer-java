//! Thin multi-symbol router: symbol → OrderBook. Sequencing stays per-book
//! (same contract as the single-symbol core).
package io.github.abhijitkrm.matcher;

import io.github.abhijitkrm.matcher.Types.Command;
import java.util.HashMap;
import java.util.Map;

public final class Engine {
    private final OrderBook.Config cfg;
    private final Map<Long, OrderBook> books = new HashMap<>();

    public Engine(OrderBook.Config cfg) { this.cfg = cfg; }

    /// Book for `symbol`, created on first use.
    public OrderBook book(long symbol) {
        return books.computeIfAbsent(symbol, k -> new OrderBook(cfg));
    }

    public void apply(long symbol, Command cmd, Sink sink) {
        book(symbol).apply(cmd, sink);
    }

    /// submit with symbol-tagged delivery: f(symbol, seq, event).
    public void applyTagged(long symbol, Command cmd, TaggedSink f) {
        book(symbol).apply(cmd, (seq, ev) -> f.onEvent(symbol, seq, ev));
    }

    public int symbols() { return books.size(); }

    /// Live symbols in ascending order (deterministic for snapshots).
    public long[] symbolList() {
        return books.keySet().stream().mapToLong(Long::longValue).sorted().toArray();
    }

    /// Book for `symbol` or null (snapshot iteration without creation).
    public OrderBook peek(long symbol) { return books.get(symbol); }

    /// Insert a fully-formed book (snapshot restore).
    public void addBook(long symbol, OrderBook b) { books.put(symbol, b); }

    public OrderBook.Config config() { return cfg; }

    @FunctionalInterface
    public interface TaggedSink {
        void onEvent(long symbol, long seq, Types.Event ev);
    }
}
