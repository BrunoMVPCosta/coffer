package io.github.brunomvpcosta.coffer.raft;

import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

/**
 * Durable Raft metadata. Implementations must not return until the state has
 * been made durable, because Raft replies to votes and appends based on it.
 */
public interface RaftStorage {

    RaftProtocol.PersistentState load();

    void save(RaftProtocol.PersistentState state);
}
