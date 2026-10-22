package io.github.brunomvpcosta.coffer.raft;

import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

/**
 * The replicated state machine applied in Raft log order.
 */
public interface RaftStateMachine {

    void apply(RaftProtocol.LogEntry entry);

    /**
     * Serializes the state at the last applied index.
     */
    byte[] snapshot();

    /**
     * Replaces the current state with a snapshot.
     */
    void restore(byte[] snapshot);
}
