package com.netty.echo.cluster;

/**
 * Membership state of a node.
 *
 * <p>State precedence is used to break ties between two observations carrying the
 * same incarnation: a higher ordinal wins. This lets a {@code DEAD} observation
 * override a concurrent {@code ALIVE} one without waiting for a new incarnation.</p>
 */
public enum MemberState {
    ALIVE,
    SUSPECT,
    DEAD;

    public boolean supersedes(MemberState other) {
        return ordinal() > other.ordinal();
    }
}
