package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.matcher.Types.Command;
import java.util.ArrayList;
import java.util.List;

/// Scale-out: several independent replicated groups, each owning a disjoint set
/// of symbols (spec/REPLICATION.md §9, decision D1; port of orderer-rust's
/// shard.rs). One replicated log is bounded by what its leader can replicate;
/// sharding multiplies that bound at the cost of a global ingress order: the
/// commands of one symbol are totally ordered (they all go to one group), but
/// there is no order between symbols in different groups.
///
/// The mix is deliberately different from the pipeline's own
/// symbol-to-partition hash, so a group's symbols still spread over its partitions.
public final class ShardMap {
    private final int groups;

    public ShardMap(int groups) {
        if (groups < 1) throw new IllegalArgumentException("at least one group");
        this.groups = groups;
    }

    public int groups() { return groups; }

    /// The group that owns sym (splitmix64 finalizer, same as the other ports).
    public int groupOf(long sym) {
        long z = (sym & 0xFFFFFFFFL) + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        z ^= z >>> 31;
        return (int) Long.remainderUnsigned(z, groups);
    }

    /// One group's part of a split batch.
    public record Part(List<Long> syms, List<Command> cmds) {
        public long[] symArray() { return syms.stream().mapToLong(Long::longValue).toArray(); }
        public Command[] cmdArray() { return cmds.toArray(new Command[0]); }
        public boolean isEmpty() { return cmds.isEmpty(); }
    }

    /// Divide a batch into one part per group. Each symbol's commands keep their relative order.
    public Part[] split(long[] syms, Command[] cmds) {
        Part[] out = new Part[groups];
        for (int g = 0; g < groups; g++) out[g] = new Part(new ArrayList<>(), new ArrayList<>());
        for (int i = 0; i < cmds.length; i++) {
            Part p = out[groupOf(syms[i])];
            p.syms().add(syms[i]);
            p.cmds().add(cmds[i]);
        }
        return out;
    }
}
