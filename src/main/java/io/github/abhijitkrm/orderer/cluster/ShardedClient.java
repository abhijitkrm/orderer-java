package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.matcher.Types.Command;
import java.util.List;

/// One leader-following Client per replica group. A failure in one group does
/// not undo another's commit: the caller sees exactly which sub-batches are committed.
public final class ShardedClient implements AutoCloseable {
    /// One group's result for a split batch; result is null if the batch had no commands for it.
    public record Outcome(Client.Result result) {
        public boolean sent() { return result != null; }
    }

    private final ShardMap map;
    private final List<Client> groups;

    public ShardedClient(List<Client> groups) { this.map = new ShardMap(groups.size()); this.groups = groups; }

    public ShardMap map() { return map; }

    /// Submit a batch split by group and report each group's outcome.
    public Outcome[] propose(long[] syms, Command[] cmds) {
        ShardMap.Part[] parts = map.split(syms, cmds);
        Outcome[] out = new Outcome[parts.length];
        for (int g = 0; g < parts.length; g++) {
            if (parts[g].isEmpty()) { out[g] = new Outcome(null); continue; }
            byte[] payload = Codec.encodeBatch(parts[g].symArray(), parts[g].cmdArray(), null);
            out[g] = new Outcome(groups.get(g).propose(payload));
        }
        return out;
    }

    @Override public void close() { for (Client c : groups) c.close(); }
}
