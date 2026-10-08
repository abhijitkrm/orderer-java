//! Preallocated order pool — indices into flat arrays, never per-order objects.
package io.github.abhijitkrm.matcher;

import io.github.abhijitkrm.matcher.Types.OType;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;

final class Pool {
    static final int NIL = -1;

    final int cap;
    int live = 0;

    final long[] id, price, qty;
    final int[] side, otype, tif, prev, next;
    private final int[] freeStack;
    private int freeTop;

    Pool(int cap) {
        this.cap = cap;
        id = new long[cap];
        price = new long[cap];
        qty = new long[cap];
        side = new int[cap];
        otype = new int[cap];
        tif = new int[cap];
        prev = new int[cap];
        next = new int[cap];
        freeStack = new int[cap];
        for (int i = 0; i < cap; i++) freeStack[i] = cap - 1 - i;
        freeTop = cap;
    }

    int alloc() {
        if (freeTop == 0) return NIL;
        live++;
        return freeStack[--freeTop];
    }

    void free(int i) {
        freeStack[freeTop++] = i;
        live--;
    }

    // ---- intrusive level link ops ----

    void levelPush(PriceIndex.Level lvl, int i) {
        prev[i] = lvl.tail;
        next[i] = NIL;
        if (lvl.tail != NIL) next[lvl.tail] = i; else lvl.head = i;
        lvl.tail = i;
    }

    /// Unlink `i` from anywhere in `lvl` and adjust the level total.
    void levelUnlink(PriceIndex.Level lvl, int i) {
        int p = prev[i], n = next[i];
        if (p != NIL) next[p] = n; else lvl.head = n;
        if (n != NIL) prev[n] = p; else lvl.tail = p;
        prev[i] = next[i] = NIL;
        lvl.total -= qty[i];
    }

    void set(int i, long oid, Side s, OType ot, Tif t, long p, long q) {
        id[i] = oid;
        side[i] = s.ordinal();
        otype[i] = ot.ordinal();
        tif[i] = t.ordinal();
        price[i] = p;
        qty[i] = q;
        prev[i] = next[i] = NIL;
    }
}
