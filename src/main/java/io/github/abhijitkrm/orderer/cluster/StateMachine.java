package io.github.abhijitkrm.orderer.cluster;

import io.github.abhijitkrm.orderer.Consensus.Entry;

/// The deterministic state machine the replicated log drives. Called only from
/// the replica's applier thread, in log order.
public interface StateMachine {
    /// Failure to restore state from a snapshot.
    final class RestoreException extends RuntimeException {
        public RestoreException(String m) { super(m); }
    }

    /// Apply committed entry index. No-op entries arrive with empty data.
    void apply(long index, Entry entry);

    /// Serialise the state after applying index.
    byte[] snapshot(long index);

    /// Replace the state with a snapshot taken at index; throws RestoreException on failure.
    void restore(long index, byte[] data);

    /// A digest of the state, for divergence detection; null = unsupported.
    default byte[] digest(long index) { return null; }

    /// A digest of the whole state, however expensive, for on-demand checks (the client
    /// protocol's state hash). Defaults to {@link #digest}.
    default byte[] deepDigest(long index) { return digest(index); }
}
