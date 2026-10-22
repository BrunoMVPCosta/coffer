package io.github.brunomvpcosta.coffer.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.github.brunomvpcosta.coffer.cluster.Node;
import io.github.brunomvpcosta.coffer.net.Address;
import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;
import io.github.brunomvpcosta.coffer.transport.MessageKind;
import io.github.brunomvpcosta.coffer.transport.Transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * In-process three-node Raft tests with a simulated network.
 */
class RaftClusterTest {

    private static final Node N1 = new Node("n1", "127.0.0.1", 8080);
    private static final Node N2 = new Node("n2", "127.0.0.1", 8081);
    private static final Node N3 = new Node("n3", "127.0.0.1", 8082);

    private final SimulatedNetwork network = new SimulatedNetwork();
    private final AtomicLong clock = new AtomicLong();
    private final List<RaftService> nodes = new ArrayList<>();
    private final List<RecordingStateMachine> machines = new ArrayList<>();

    @BeforeEach
    void setUp() {
        start(N1, N2, N3);
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftService::stop);
    }

    @Test
    void electsASingleLeader() {
        elect();

        assertEquals(1, nodes.stream().filter(RaftService::isLeader).count());
    }

    @Test
    void replicatesCommandsToEveryNode() {
        elect();
        var leader = leader();

        leader.append(bytes("x"));

        for (var i = 0; i < 5; i++) {
            tickAll();
        }

        for (var machine : machines) {
            assertTrue(machine.commands.contains("x"), "every node should apply the command");
        }
    }

    @Test
    void partitionedMinorityCannotCommit() {
        elect();
        var oldLeader = leader();
        var followers = nodes.stream().filter(node -> node != oldLeader).toList();

        network.block(oldLeader.self().id(), followers.get(0).self().id());
        network.block(oldLeader.self().id(), followers.get(1).self().id());

        var before = oldLeader.commitIndex();
        oldLeader.append(bytes("y"));
        tickAll();

        assertEquals(before, oldLeader.commitIndex(), "a minority must not commit");

        var newLeader = electAmong(followers);

        assertNotNull(newLeader);
        assertTrue(newLeader.currentTerm() > oldLeader.currentTerm());

        newLeader.append(bytes("z"));

        for (var i = 0; i < 5; i++) {
            tickAll();
        }

        assertTrue(newLeader.commitIndex() > before, "the majority should commit");

        network.heal();

        for (var i = 0; i < 10; i++) {
            tickAll();
        }

        assertEquals(1, nodes.stream().filter(RaftService::isLeader).count());
        assertEquals(newLeader.currentTerm(), oldLeader.currentTerm());
    }

    private void start(Node... definitions) {
        for (var node : definitions) {
            var peers = Arrays.stream(definitions).filter(peer -> !peer.equals(node)).toList();
            var machine = new RecordingStateMachine();
            var raft = new RaftService(
                node,
                peers,
                new NodeTransport(network, node),
                new InMemoryRaftStorage(),
                1024,
                50,
                100,
                200,
                clock::get,
                new Random(node.id().hashCode()));

            raft.setStateMachine(machine);
            network.register(node, raft);
            nodes.add(raft);
            machines.add(machine);
        }
    }

    private void elect() {
        for (var i = 0; i < 30 && leader() == null; i++) {
            tickAll();
        }

        assertNotNull(leader(), "a leader should be elected");
    }

    private RaftService electAmong(List<RaftService> candidates) {
        for (var i = 0; i < 40; i++) {
            clock.addAndGet(1000);
            candidates.forEach(RaftService::tick);

            var elected = candidates.stream().filter(RaftService::isLeader).findFirst();

            if (elected.isPresent()) {
                return elected.get();
            }
        }

        return null;
    }

    private void tickAll() {
        clock.addAndGet(1000);
        nodes.forEach(RaftService::tick);
    }

    private RaftService leader() {
        return nodes.stream().filter(RaftService::isLeader).findFirst().orElse(null);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static final class RecordingStateMachine implements RaftStateMachine {

        private final List<String> commands = new ArrayList<>();

        @Override
        public void apply(RaftProtocol.LogEntry entry) {
            if (!entry.getCommand().isEmpty()) {
                commands.add(entry.getCommand().toStringUtf8());
            }
        }

        @Override
        public byte[] snapshot() {
            return new byte[0];
        }

        @Override
        public void restore(byte[] snapshot) {
        }
    }

    static final class SimulatedNetwork {

        private final Map<String, RaftMessageHandler> handlers = new ConcurrentHashMap<>();
        private final Map<Address, String> addresses = new ConcurrentHashMap<>();
        private final Set<String> blocked = ConcurrentHashMap.newKeySet();

        void register(Node node, RaftMessageHandler handler) {
            handlers.put(node.id(), handler);
            addresses.put(new Address(node.host(), node.port()), node.id());
        }

        RaftMessageHandler handler(String id) {
            return handlers.get(id);
        }

        String idFor(Address address) {
            return addresses.get(address);
        }

        void block(String from, String to) {
            blocked.add(from + "|" + to);
            blocked.add(to + "|" + from);
        }

        void heal() {
            blocked.clear();
        }

        boolean isBlocked(String from, String to) {
            return blocked.contains(from + "|" + to);
        }
    }

    static final class NodeTransport implements Transport {

        private final SimulatedNetwork network;
        private final Node self;

        NodeTransport(SimulatedNetwork network, Node self) {
            this.network = network;
            this.self = self;
        }

        @Override
        public void send(Address address, byte[] payload, MessageKind kind) {
            deliver(address, payload);
        }

        @Override
        public CompletableFuture<byte[]> sendAndReceive(Address address, byte[] payload, MessageKind kind) {
            var target = network.idFor(address);

            if (target == null || network.isBlocked(self.id(), target)) {
                return CompletableFuture.failedFuture(new IllegalStateException("unreachable"));
            }

            try {
                var request = RaftProtocol.RaftMessage.parseFrom(payload);
                var response = network.handler(target).handle(request);
                return CompletableFuture.completedFuture(
                    response == null ? new byte[0] : response.toByteArray());
            } catch (Exception ex) {
                return CompletableFuture.failedFuture(ex);
            }
        }

        private void deliver(Address address, byte[] payload) {
            var target = network.idFor(address);

            if (target == null || network.isBlocked(self.id(), target)) {
                return;
            }

            try {
                network.handler(target).handle(RaftProtocol.RaftMessage.parseFrom(payload));
            } catch (Exception ignored) {
                // fire-and-forget messages are best effort
            }
        }
    }
}
