package com.netty.echo.raft;

import com.netty.echo.protocol.RaftProtocol;

/**
 * Non-durable storage for tests and ephemeral single-node runs.
 */
public class InMemoryRaftStorage implements RaftStorage {

    private RaftProtocol.PersistentState state = RaftProtocol.PersistentState.getDefaultInstance();

    @Override
    public synchronized RaftProtocol.PersistentState load() {
        return state;
    }

    @Override
    public synchronized void save(RaftProtocol.PersistentState state) {
        this.state = state;
    }
}
