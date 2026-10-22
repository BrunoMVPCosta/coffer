package io.github.brunomvpcosta.coffer.raft;

/**
 * Thrown when a leader-only operation is attempted on a follower.
 */
public class RaftNotLeaderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String leaderId;

    public RaftNotLeaderException(String leaderId) {
        super("not the leader");
        this.leaderId = leaderId;
    }

    public String leaderId() {
        return leaderId;
    }
}
