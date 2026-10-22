package io.github.brunomvpcosta.coffer.cluster;

/**
 * A node together with its membership metadata.
 *
 * <p>Every node keeps its own incarnation number, which it increments when it
 * needs to refute a stale observation about itself (for example when it hears
 * that it is dead). A membership update is accepted only when it is newer than
 * what is already known:</p>
 *
 * <ul>
 *   <li>a higher incarnation always wins;</li>
 *   <li>at the same incarnation, a higher state wins ({@code DEAD} over
 *       {@code SUSPECT} over {@code ALIVE});</li>
 *   <li>at the same incarnation and state, an address change is accepted.</li>
 * </ul>
 */
public record Member(Node node, long incarnation, MemberState state) implements Comparable<Member> {

    public Member(Node node) {
        this(node, 1, MemberState.ALIVE);
    }

    public Member {
        if (node == null) {
            throw new IllegalArgumentException("node cannot be null");
        }
        if (state == null) {
            throw new IllegalArgumentException("state cannot be null");
        }
    }

    public static Member of(Node node) {
        return new Member(node);
    }

    public boolean isAlive() {
        return state == MemberState.ALIVE;
    }

    public boolean isDead() {
        return state == MemberState.DEAD;
    }

    /**
     * @return true if this observation should replace {@code other}
     */
    public boolean supersedes(Member other) {
        if (other == null) {
            return true;
        }

        if (incarnation != other.incarnation) {
            return incarnation > other.incarnation;
        }

        if (state != other.state) {
            return state.supersedes(other.state);
        }

        return !node.equals(other.node);
    }

    @Override
    public int compareTo(Member other) {
        return Long.compare(incarnation, other.incarnation);
    }
}
