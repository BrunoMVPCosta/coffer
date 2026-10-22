package io.github.brunomvpcosta.coffer.cluster;

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
    void mergeKeepsExistingMembers() {
        var existing = new Node("10.0.0.9:8099", "10.0.0.9", 8099);
        cluster.addMember(existing);

        var other = new Node("10.0.0.3:8082", "10.0.0.3", 8082);
        cluster.merge(List.of(Member.of(remote), Member.of(other), Member.of(local)));

        assertEquals(3, cluster.getMembers().size());
        assertEquals(existing, cluster.getMember(existing.id()));
        assertEquals(other, cluster.getMember(other.id()));
    }

    @Test
    void mergeIgnoresOlderIncarnation() {
        cluster.merge(List.of(new Member(remote, 5, MemberState.ALIVE)));

        cluster.merge(List.of(new Member(remote, 4, MemberState.DEAD)));

        assertEquals(remote, cluster.getMember(remote.id()));
    }

    @Test
    void mergeMarksMemberDeadAtSameIncarnation() {
        cluster.merge(List.of(new Member(remote, 5, MemberState.ALIVE)));

        cluster.merge(List.of(new Member(remote, 5, MemberState.DEAD)));

        assertNull(cluster.getMember(remote.id()));
        assertEquals(MemberState.DEAD, membership(remote).state());
    }

    @Test
    void mergeHigherIncarnationResurrectsMember() {
        cluster.merge(List.of(new Member(remote, 5, MemberState.DEAD)));

        cluster.merge(List.of(new Member(remote, 6, MemberState.ALIVE)));

        assertEquals(remote, cluster.getMember(remote.id()));
    }

    @Test
    void mergeRefutesTombstoneAboutLocalNode() {
        cluster.merge(List.of(new Member(local, 1, MemberState.DEAD)));

        var localMember = cluster.getLocalMember();

        assertEquals(2, localMember.incarnation());
        assertTrue(localMember.isAlive());
    }

    @Test
    void staleMembershipCannotResurrectDeadMember() {
        cluster.merge(List.of(new Member(remote, 7, MemberState.ALIVE)));

        cluster.removeMember(remote.id());

        cluster.merge(List.of(new Member(remote, 7, MemberState.ALIVE)));

        assertNull(cluster.getMember(remote.id()));
        assertEquals(MemberState.DEAD, membership(remote).state());
    }

    @Test
    void pruneDeadDropsTombstones() {
        cluster.merge(List.of(new Member(remote, 7, MemberState.ALIVE)));
        cluster.removeMember(remote.id());

        cluster.pruneDead(-1);

        assertNull(membership(remote));
    }

    private Member membership(Node node) {
        return cluster.getMembership().stream()
            .filter(member -> member.node().id().equals(node.id()))
            .findFirst()
            .orElse(null);
    }
}
