//! Price index: direct-indexed bitmap ladder for bounded ranges (O(1) best
//! price via a top-of-book cursor), TreeMap fallback for unbounded prices.
//! Enum-dispatched — no virtuals in the match loop.
package io.github.abhijitkrm.matcher;

import io.github.abhijitkrm.matcher.Types.Side;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public final class PriceIndex {
    static final int NIL = -1;

    static final class Level {
        int head = NIL, tail = NIL;
        long total = 0;
        boolean empty() { return head == NIL; }
    }

    public record Depth(long price, long qty) {}

    public enum Kind { Ladder, Tree }

    final Kind kind;
    final Ladder lad;
    final Tree tree;

    private PriceIndex(Kind k, Side s, long pmin, long pmax) {
        kind = k;
        lad = new Ladder(s, pmin, pmax);
        tree = new Tree(s);
    }

    static PriceIndex ladder(Side s, long pmin, long pmax) {
        return new PriceIndex(Kind.Ladder, s, pmin, pmax);
    }
    static PriceIndex tree(Side s) { return new PriceIndex(Kind.Tree, s, 0, 0); }

    private Impl impl() { return kind == Kind.Ladder ? lad : tree; }

    long bestPrice() { return impl().bestPrice(); }       // Long.MIN_VALUE = none
    Level levelMut(long p) { return impl().levelMut(p); }
    Level levelInsert(long p) { return impl().levelInsert(p); }
    void unlinkLevel(long p) { impl().unlinkLevel(p); }
    long sumRange(long lo, long hi) { return impl().sumRange(lo, hi); }
    int len() { return impl().len(); }
    List<Depth> depth(int n) { return impl().depth(n); }

    private interface Impl {
        long bestPrice();
        Level levelMut(long p);
        Level levelInsert(long p);
        void unlinkLevel(long p);
        long sumRange(long lo, long hi);
        int len();
        List<Depth> depth(int n);
    }

    // ---- ladder: levels[p - base] + occupancy bitmap + best-price cursor ----

    private static final class Ladder implements Impl {
        final Side side;
        final long base;
        final Level[] levels;
        final long[] bits;
        int best = NIL;
        int count = 0;

        Ladder(Side side, long pmin, long pmax) {
            this.side = side;
            this.base = pmin;
            int span = (int) (pmax - pmin + 1);
            levels = new Level[span];
            for (int i = 0; i < span; i++) levels[i] = new Level();
            bits = new long[(span + 63) >> 6];
        }

        private int idx(long p) { return (int) (p - base); }

        public long bestPrice() { return best == NIL ? Long.MIN_VALUE : base + best; }

        public Level levelMut(long p) {
            int i = idx(p);
            if (i < 0 || i >= levels.length) return null;
            Level l = levels[i];
            return l.empty() ? null : l;
        }

        public Level levelInsert(long p) {
            int i = idx(p);
            Level l = levels[i];
            if (l.empty()) {
                bits[i >> 6] |= 1L << (i & 63);
                count++;
                if (best == NIL || (side == Side.Ask ? i < best : i > best)) best = i;
            }
            return l;
        }

        public void unlinkLevel(long p) {
            int i = idx(p);
            if (i < 0 || i >= levels.length || !levels[i].empty()) return;
            bits[i >> 6] &= ~(1L << (i & 63));
            count--;
            if (i == best) best = rescan(i);
        }

        /// Nearest non-empty level strictly beyond `from`; NIL if emptied.
        private int rescan(int from) {
            int n = levels.length;
            if (side == Side.Ask) {
                for (int w = from >> 6; w < bits.length; w++) {
                    long word = bits[w];
                    if (w == from >> 6) {
                        int b = (from & 63) + 1;
                        word &= b == 64 ? 0L : ~0L << b;
                    }
                    if (word != 0) {
                        int i = (w << 6) + Long.numberOfTrailingZeros(word);
                        return i < n ? i : NIL;
                    }
                }
            } else {
                for (int w = from >> 6; w >= 0; w--) {
                    long word = bits[w];
                    if (w == from >> 6) {
                        int b = from & 63;
                        word &= b == 0 ? 0L : ~0L >>> (64 - b);
                    }
                    if (word != 0) {
                        int i = (w << 6) + (63 - Long.numberOfLeadingZeros(word));
                        return i < n ? i : NIL;
                    }
                }
            }
            return NIL;
        }

        public long sumRange(long lo, long hi) {
            int loI = (int) Math.max(0, idx(lo));
            int hiI = Math.min(levels.length - 1, idx(hi));
            long sum = 0;
            for (int i = loI; i <= hiI; i++) sum += levels[i].total;
            return sum;
        }

        public int len() { return count; }

        public List<Depth> depth(int n) {
            List<Depth> out = new ArrayList<>(Math.min(n, 16));
            if (side == Side.Ask) {
                for (int w = 0; w < bits.length && out.size() < n; w++) {
                    long word = bits[w];
                    while (word != 0 && out.size() < n) {
                        int i = (w << 6) + Long.numberOfTrailingZeros(word);
                        out.add(new Depth(base + i, levels[i].total));
                        word &= word - 1;
                    }
                }
            } else {
                for (int w = bits.length - 1; w >= 0 && out.size() < n; w--) {
                    long word = bits[w];
                    while (word != 0 && out.size() < n) {
                        int i = (w << 6) + (63 - Long.numberOfLeadingZeros(word));
                        out.add(new Depth(base + i, levels[i].total));
                        word &= ~(1L << (63 - Long.numberOfLeadingZeros(word)));
                    }
                }
            }
            return out;
        }
    }

    // ---- tree: TreeMap fallback for unbounded prices ----

    private static final class Tree implements Impl {
        final Side side;
        final TreeMap<Long, Level> map = new TreeMap<>();

        Tree(Side side) { this.side = side; }

        public long bestPrice() {
            if (map.isEmpty()) return Long.MIN_VALUE;
            return side == Side.Bid ? map.lastKey() : map.firstKey();
        }

        public Level levelMut(long p) {
            Level l = map.get(p);
            return l == null || l.empty() ? null : l;
        }

        public Level levelInsert(long p) {
            return map.computeIfAbsent(p, k -> new Level());
        }

        public void unlinkLevel(long p) {
            Level l = map.get(p);
            if (l != null && l.empty()) map.remove(p);
        }

        public long sumRange(long lo, long hi) {
            long sum = 0;
            for (Level l : map.subMap(lo, true, hi, true).values()) sum += l.total;
            return sum;
        }

        public int len() { return map.size(); }

        public List<Depth> depth(int n) {
            List<Depth> out = new ArrayList<>(Math.min(n, 16));
            var it = (side == Side.Ask ? map : map.descendingMap()).entrySet().iterator();
            while (it.hasNext() && out.size() < n) {
                var e = it.next();
                out.add(new Depth(e.getKey(), e.getValue().total));
            }
            return out;
        }
    }
}
