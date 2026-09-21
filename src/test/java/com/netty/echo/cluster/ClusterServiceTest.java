package com.netty.echo.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import com.netty.echo.net.Address;
import com.netty.echo.protocol.ClusterProtocol;
import com.netty.echo.transport.MessageKind;
import com.netty.echo.transport.Transport;

import org.junit.jupiter.api.Test;

/**
 * ClusterServiceTest
 */
class ClusterServiceTest {

    private final Node local = new Node("127.0.0.1:8080", "127.0.0.1", 8080);
    private final Node remote = new Node("10.0.0.2:8081", "10.0.0.2", 8081);
    private final DefaultCluster cluster = new DefaultCluster("test-cluster", local);
    private final FakeTransport transport = new FakeTransport();

    @Test
    void joinAddsMemberAndReturnsWelcomeWithAllMembers() {
        var service = new ClusterService(cluster, transport, List.of());

        var response = service.handle(ClusterMessages.join("test-cluster", remote));

        assertTrue(response.hasWelcome());
        assertEquals(remote, cluster.getMember(remote.id()));

        var members = response.getWelcome().getMembersList().stream()
            .map(ClusterMessages::fromProto)
            .toList();

        assertEquals(List.of(local, remote), members);
    }

    @Test
    void joinBroadcastsMembersToExistingMembers() throws Exception {
        cluster.addMember(new Node("10.0.0.3:8082", "10.0.0.3", 8082));
        var service = new ClusterService(cluster, transport, List.of());

        service.handle(ClusterMessages.join("test-cluster", remote));

        assertEquals(2, transport.sent.size());
        var pushed = ClusterProtocol.ClusterMessage.parseFrom(transport.sent.get(0).payload());
        assertTrue(pushed.hasMembers());
    }

    @Test
    void pingIsAnsweredWithPong() {
        var service = new ClusterService(cluster, transport, List.of());

        var response = service.handle(ClusterMessages.ping(remote));

        assertTrue(response.hasPong());
        assertEquals(local.id(), response.getPong().getSender().getId());
    }

    @Test
    void membersMessageReplacesMembers() {
        var service = new ClusterService(cluster, transport, List.of());

        service.handle(ClusterMessages.members("test-cluster", List.of(remote)));

        assertEquals(List.of(remote), cluster.getMembers());
    }

    @Test
    void leaveRemovesMember() {
        cluster.addMember(remote);
        var service = new ClusterService(cluster, transport, List.of());

        service.handle(ClusterMessages.leave(remote.id()));

        assertTrue(cluster.getMembers().isEmpty());
    }

    @Test
    void startJoinsFirstReachableSeed() throws Exception {
        var joined = new Node("10.0.0.4:8083", "10.0.0.4", 8083);
        transport.response = ClusterMessages.welcome("test-cluster", List.of(joined)).toByteArray();
        transport.unreachable = Address.from("10.0.0.9:9999");

        var service = new ClusterService(cluster, transport,
            List.of(Address.from("10.0.0.9:9999"), Address.from("10.0.0.4:8083")));

        try {
            service.start();

            assertEquals(joined, cluster.getMember(joined.id()));
        } finally {
            service.leave();
        }
    }

    static class FakeTransport implements Transport {

        record Sent(Address address, byte[] payload) {
        }

        volatile byte[] response = new byte[0];
        volatile Address unreachable;
        final List<Sent> sent = new CopyOnWriteArrayList<>();

        @Override
        public void send(Address address, byte[] payload, MessageKind kind) {
            sent.add(new Sent(address, payload));
        }

        @Override
        public CompletableFuture<byte[]> sendAndReceive(Address address, byte[] payload, MessageKind kind) {
            if (address.equals(unreachable)) {
                return CompletableFuture.failedFuture(new RuntimeException("unreachable"));
            }

            return CompletableFuture.completedFuture(response);
        }
    }
}
