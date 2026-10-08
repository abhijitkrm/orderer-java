//! Open-addressed order-id → pool-index map. Fibonacci hash with high-bit
//! fold (sequential ids distribute evenly), power-of-two table, linear probe,
//! backward-shift delete (no tombstones).
package io.github.abhijitkrm.matcher;

final class OrderMap {
    private long[] keys;
    private int[] vals;
    private byte[] used;
    private int mask;
    private int count = 0;

    OrderMap(int capHint) {
        int cap = 16;
        while (cap < capHint * 2) cap <<= 1;
        keys = new long[cap];
        vals = new int[cap];
        used = new byte[cap];
        mask = cap - 1;
    }

    private static int mix(long id) {
        long h = id * 0x9E3779B97F4A7C15L;
        h ^= h >>> 33;
        return (int) h;
    }

    boolean contains(long id) { return find(id) >= 0; }

    int get(long id) {
        int i = find(id);
        return i < 0 ? -1 : vals[i];
    }

    private int find(long id) {
        int i = mix(id) & mask;
        for (;;) {
            if (used[i] == 0) return -1;
            if (keys[i] == id) return i;
            i = (i + 1) & mask;
        }
    }

    void insert(long id, int val) {
        if (count * 4 >= keys.length * 3) grow();
        int i = mix(id) & mask;
        while (used[i] != 0) i = (i + 1) & mask;
        used[i] = 1;
        keys[i] = id;
        vals[i] = val;
        count++;
    }

    void remove(long id) {
        int i = find(id);
        if (i < 0) return;
        for (;;) {
            int j = i;
            used[i] = 0;
            for (;;) {
                j = (j + 1) & mask;
                if (used[j] == 0) { count--; return; }
                int k = mix(keys[j]) & mask;
                // j may move to i only if its ideal slot k is NOT in cyclic (i, j].
                boolean kInPath = i <= j ? k > i && k <= j : k > i || k <= j;
                if (!kInPath) break;
            }
            keys[i] = keys[j];
            vals[i] = vals[j];
            used[i] = 1;
            i = j;
        }
    }

    private void grow() {
        long[] ok = keys; int[] ov = vals; byte[] ou = used;
        int cap = ok.length << 1;
        keys = new long[cap]; vals = new int[cap]; used = new byte[cap];
        mask = cap - 1; count = 0;
        for (int i = 0; i < ou.length; i++) if (ou[i] != 0) insert(ok[i], ov[i]);
    }
}
