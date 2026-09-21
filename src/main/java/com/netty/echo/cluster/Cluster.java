package com.netty.echo.cluster;

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
     * @return returns the member with a specific id
     */
    Node getMember(String id);

    /**
     * Returns a list of nodes in the clsuter
     */
    Collection<Node> getMembers();

    /**
     * Adds a new member to the cluster
     * @param node
     */
    void addMember(Node node);

    void removeMember(String id);

    void setMembers(Collection<Node> members);
}
