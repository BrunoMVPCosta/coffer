package io.github.brunomvpcosta.coffer.raft;

import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

/**
 * Durable Raft metadata and snapshots.
 *
 * <p>Implementations must not return from {@code save} until the state is
 * durable. Snapshots are stored separately from the log so a large snapshot is
 * not rewritten on every log append.</p>
 */
public interface RaftStorage {

    RaftProtocol.PersistentState load();

    void save(RaftProtocol.PersistentState state);

    RaftProtocol.Snapshot loadSnapshot();

    void saveSnapshot(RaftProtocol.Snapshot snapshot);
}
