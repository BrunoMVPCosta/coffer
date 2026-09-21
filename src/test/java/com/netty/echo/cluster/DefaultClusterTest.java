package com.netty.echo.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * DefaultClusterTest
 */
class DefaultClusterTest {

    private final Node local = new Node("127.0.0.1:8080", "127.0.0.1", 8080);
    private final Node remote = new Node("10.0.0.2:8081", "10.0.0.2", 8081);
    private final Cluster cluster = new DefaultCluster("test-cluster", local);

    @Test
    void addMemberAddsRemoteNode() {
        cluster.addMember(remote);

        assertEquals(List.of(remote), cluster.getMembers());
    }

    @Test
    void addMemberIgnoresLocalNode() {
        cluster.addMember(local);

        assertTrue(cluster.getMembers().isEmpty());
    }

    @Test
    void addMemberUpdatesNodeWithSameId() {
        cluster.addMember(remote);
        cluster.addMember(new Node(remote.id(), "10.0.0.9", 8081));

        assertEquals("10.0.0.9", cluster.getMember(remote.id()).host());
        assertEquals(1, cluster.getMembers().size());
    }

    @Test
    void getMemberReturnsLocalAndRemoteNodes() {
        cluster.addMember(remote);

        assertEquals(local, cluster.getMember(local.id()));
        assertEquals(remote, cluster.getMember(remote.id()));
        assertNull(cluster.getMember("unknown"));
    }

    @Test
    void removeMemberRemovesById() {
        cluster.addMember(remote);

        cluster.removeMember(remote.id());

        assertTrue(cluster.getMembers().isEmpty());
    }

    @Test
    void setMembersReplacesExistingOnesAndSkipsLocalNode() {
        cluster.addMember(new Node("10.0.0.9:8099", "10.0.0.9", 8099));

        var other = new Node("10.0.0.3:8082", "10.0.0.3", 8082);
        cluster.setMembers(List.of(remote, other, local));

        assertEquals(2, cluster.getMembers().size());
        assertNull(cluster.getMember("10.0.0.9:8099"));
        assertEquals(other, cluster.getMember(other.id()));
    }
}
