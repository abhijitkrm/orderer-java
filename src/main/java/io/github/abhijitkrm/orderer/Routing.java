//! Symbol → partition (spec/ROUTING.md).
package io.github.abhijitkrm.orderer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Routing {
    private Routing() {}

    public static final int MAX_PARTITIONS = 1024;

    /// spec/ROUTING.md §2. Java has no unsigned long: the multiply wraps
    /// identically, and every shift is unsigned (>>>).
    public static int hashPartition(long symbol, int partitions) {
        long h = (symbol & 0xFFFFFFFFL) * 0x9E3779B97F4A7C15L;
        return (int) (((h >>> 32) * partitions) >>> 32);
    }

    public static final class RoutingError extends RuntimeException {
        public RoutingError(String m) { super(m); }
    }

    /// A fixed map: table overrides, hash for the rest.
    public static final class PartitionMap {
        private static final int DENSE = 4096;
        private final int p;
        private final int[] dense = new int[DENSE];
        private final Map<Long, Integer> sparse = new HashMap<>();

        public PartitionMap(int partitions, List<long[]> table) {
            if (partitions < 1 || partitions > MAX_PARTITIONS)
                throw new RoutingError("partitions must be 1..=" + MAX_PARTITIONS + ", got " + partitions);
            p = partitions;
            for (int s = 0; s < DENSE; s++) dense[s] = hashPartition(s, partitions);
            Set<Long> seen = new HashSet<>();
            for (long[] e : table) {
                long sym = e[0], part = e[1];
                if (part >= partitions)
                    throw new RoutingError("symbol " + sym + ": partition " + part + " out of range for " + partitions + " partitions");
                if (!seen.add(sym)) throw new RoutingError("symbol " + sym + " listed twice");
                if (sym < DENSE) dense[(int) sym] = (int) part;
                sparse.put(sym, (int) part);
            }
        }

        public PartitionMap(int partitions) { this(partitions, List.of()); }

        /// Parse an orderer-partition-map/1 file for a pipeline of `partitions`.
        public static PartitionMap parseTable(String text, int partitions) {
            List<long[]> table = new java.util.ArrayList<>();
            boolean first = true;
            for (String raw : text.split("\n")) {
                String line = raw.trim();
                if (line.isEmpty()) continue;
                if (first) {
                    first = false;
                    if (!"orderer-partition-map/1".equals(Flat.get(line, "format")))
                        throw new RoutingError("not an orderer-partition-map/1 header");
                    Long pp = Flat.u64(line, "partitions");
                    if (pp == null) throw new RoutingError("partition map header lacks partitions");
                    if (pp != partitions)
                        throw new RoutingError("partition map is for " + pp + " partitions, pipeline has " + partitions);
                    continue;
                }
                Long s = Flat.u64(line, "symbol"), q = Flat.u64(line, "partition");
                if (s == null || s < 0 || s > 0xFFFFFFFFL) throw new RoutingError("bad symbol in: " + line);
                if (q == null || q < 0 || q > 0xFFFFFFFFL) throw new RoutingError("bad partition in: " + line);
                table.add(new long[] {s, q});
            }
            if (first) throw new RoutingError("empty partition map");
            return new PartitionMap(partitions, table);
        }

        public int partitions() { return p; }

        public int partition(long sym) {
            if (sym >= 0 && sym < DENSE) return dense[(int) sym];
            if (!sparse.isEmpty()) {
                Integer x = sparse.get(sym);
                if (x != null) return x;
            }
            return hashPartition(sym, p);
        }
    }
}
