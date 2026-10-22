package io.github.brunomvpcosta.coffer.raft;

import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

/**
 * Non-durable storage for tests and ephemeral single-node runs.
 */
public class InMemoryRaftStorage implements RaftStorage {

    private RaftProtocol.PersistentState state = RaftProtocol.PersistentState.getDefaultInstance();
    private RaftProtocol.Snapshot snapshot = RaftProtocol.Snapshot.getDefaultInstance();

    @Override
    public synchronized RaftProtocol.PersistentState load() {
        return state;
    }

    @Override
    public synchronized void save(RaftProtocol.PersistentState state) {
        this.state = state;
    }

    @Override
    public synchronized RaftProtocol.Snapshot loadSnapshot() {
        return snapshot;
    }

    @Override
    public synchronized void saveSnapshot(RaftProtocol.Snapshot snapshot) {
        this.snapshot = snapshot;
    }
}
