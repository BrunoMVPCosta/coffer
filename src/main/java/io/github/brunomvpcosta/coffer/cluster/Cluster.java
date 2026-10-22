package io.github.brunomvpcosta.coffer.cluster;

import java.util.Collection;

/**
 * Cluster
 */
public interface Cluster {

    /**
     *
     * @return the cluster's name
     */
    String getName();

    /**
     *
     * @return the local member of the cluster
     */
    Node getMember();

    /**
     *
     * @param id
     * @return returns the live member with a specific id, or null
     */
    Node getMember(String id);

    /**
     * Returns the live nodes in the cluster (the local node is not included)
     */
    Collection<Node> getMembers();

    /**
     * Returns every known node, including the local one and dead tombstones.
     */
    Collection<Member> getMembership();

    /**
     * @return the local node with its current incarnation and state
     */
    Member getLocalMember();

    /**
     * Adds a new member to the cluster using a fresh incarnation.
     * @param node
     */
    void addMember(Node node);

    /**
     * Marks a member as dead using the last known incarnation.
     */
    void removeMember(String id);

    /**
     * Marks a member as dead using the supplied incarnation, so a stale
     * observation of that member cannot resurrect it later.
     */
    void removeMember(String id, long incarnation);

    /**
     * Merges membership observations, keeping the newest one per node.
     *
     * @return true when the local incarnation was bumped to refute a stale
     *         observation about the local node, or when the membership changed
     */
    boolean merge(Collection<Member> members);

    /**
     * Drops dead tombstones older than the given age.
     */
    void pruneDead(long ttlMillis);
}
