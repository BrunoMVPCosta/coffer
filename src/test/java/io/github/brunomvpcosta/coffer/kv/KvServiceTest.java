package io.github.brunomvpcosta.coffer.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;
import io.github.brunomvpcosta.coffer.cluster.Node;
import io.github.brunomvpcosta.coffer.net.Address;
import io.github.brunomvpcosta.coffer.protocol.KvProtocol;
import io.github.brunomvpcosta.coffer.raft.InMemoryRaftStorage;
import io.github.brunomvpcosta.coffer.raft.RaftMessages;
import io.github.brunomvpcosta.coffer.raft.RaftService;
import io.github.brunomvpcosta.coffer.transport.MessageKind;
import io.github.brunomvpcosta.coffer.transport.Transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KvServiceTest {

    private static final Node LOCAL = new Node("n1", "127.0.0.1", 8080);
    private static final Node PEER2 = new Node("n2", "127.0.0.1", 8081);

    private final FakeTransport transport = new FakeTransport();
    private final AtomicLong clock = new AtomicLong();
    private final InMemoryRaftStorage storage = new InMemoryRaftStorage();

    private RaftService raft;
    private KvService kv;

    @AfterEach
    void stop() {
        if (raft != null) {
            raft.stop();
        }
    }

    @Test
    void leaderAppliesPutGetAndDelete() throws Exception {
        startSingleNodeLeader();

        assertEquals(KvProtocol.KvResponse.Status.STATUS_OK, call(put("a", "1")).getStatus());

        var read = call(get("a"));

        assertEquals(KvProtocol.KvResponse.Status.STATUS_OK, read.getStatus());
        assertTrue(read.getHasValue());
        assertEquals("1", read.getValue().toStringUtf8());

        assertEquals(KvProtocol.KvResponse.Status.STATUS_OK, call(delete("a")).getStatus());
        assertEquals(KvProtocol.KvResponse.Status.STATUS_NOT_FOUND, call(get("a")).getStatus());
    }

    @Test
    void writeOnFollowerIsForwardedToKnownLeader() throws Exception {
        createRaft(PEER2);
        raft.handle(RaftMessages.appendEntries(1, PEER2.id(), 0, 0, List.of(), 0));

        var result = kv.handle(put("a", "1"));
        var forwarded = transport.last(MessageKind.KV);

        assertEquals(new Address(PEER2.host(), PEER2.port()), forwarded.address());

        forwarded.response().complete(KvProtocol.KvResponse.newBuilder()
            .setStatus(KvProtocol.KvResponse.Status.STATUS_OK)
            .build()
            .toByteArray());

        assertEquals(KvProtocol.KvResponse.Status.STATUS_OK, result.get(1, TimeUnit.SECONDS).getStatus());
    }

    @Test
    void writeWithoutLeaderReturnsNotLeader() throws Exception {
        createRaft();

        var response = call(put("a", "1"));

        assertEquals(KvProtocol.KvResponse.Status.STATUS_NOT_LEADER, response.getStatus());
    }

    @Test
    void infoReportsRoleTermLeaderAndKeys() throws Exception {
        startSingleNodeLeader();
        call(put("b", "2"));
        call(put("a", "1"));

        var info = call(info());

        assertEquals(KvProtocol.KvResponse.Status.STATUS_OK, info.getStatus());
        assertEquals("LEADER", info.getRole());
        assertEquals(1, info.getTerm());
        assertEquals(LOCAL.id(), info.getLeaderId());
        assertEquals(List.of("a", "b"), info.getKeysList());
    }

    @Test
    void restartReplaysCommittedLogIntoStateMachine() throws Exception {
        startSingleNodeLeader();
        call(put("a", "1"));

        raft = new RaftService(LOCAL, List.of(), transport, storage,
            50, 100, 200, clock::get, new Random(7));
        kv = new KvService(raft, transport, new KvStateMachine());

        clock.set(20_000);
        raft.tick();

        assertTrue(raft.isLeader());

        var read = call(get("a"));

        assertEquals(KvProtocol.KvResponse.Status.STATUS_OK, read.getStatus());
        assertEquals("1", read.getValue().toStringUtf8());
    }

    @Test
    void snapshotRestoresKvStateAfterRestart() throws Exception {
        createRaft(2);
        clock.set(10_000);
        raft.tick();
        call(put("a", "1"));

        assertEquals(2, storage.loadSnapshot().getLastIncludedIndex());
        assertEquals(0, raft.logSize());

        raft = new RaftService(LOCAL, List.of(), transport, storage, 2,
            50, 100, 200, clock::get, new Random(7));
        kv = new KvService(raft, transport, new KvStateMachine());

        var read = call(get("a"));

        assertEquals(KvProtocol.KvResponse.Status.STATUS_OK, read.getStatus());
        assertEquals("1", read.getValue().toStringUtf8());
    }

    private void startSingleNodeLeader() {
        createRaft();
        clock.set(10_000);
        raft.tick();
        assertTrue(raft.isLeader());
    }

    private void createRaft(Node... peers) {
        createRaft(1024, peers);
    }

    private void createRaft(long snapshotThreshold, Node... peers) {
        raft = new RaftService(LOCAL, List.of(peers), transport, storage,
            snapshotThreshold, 50, 100, 200, clock::get, new Random(7));
        kv = new KvService(raft, transport, new KvStateMachine());
    }

    private KvProtocol.KvResponse call(KvProtocol.KvCommand command) throws Exception {
        return kv.handle(command).get(1, TimeUnit.SECONDS);
    }

    private static KvProtocol.KvCommand put(String key, String value) {
        return KvProtocol.KvCommand.newBuilder()
            .setPut(KvProtocol.Put.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8))))
            .build();
    }

    private static KvProtocol.KvCommand get(String key) {
        return KvProtocol.KvCommand.newBuilder()
            .setGet(KvProtocol.Get.newBuilder().setKey(key))
            .build();
    }

    private static KvProtocol.KvCommand delete(String key) {
        return KvProtocol.KvCommand.newBuilder()
            .setDelete(KvProtocol.Delete.newBuilder().setKey(key))
            .build();
    }

    private static KvProtocol.KvCommand info() {
        return KvProtocol.KvCommand.newBuilder()
            .setInfo(KvProtocol.Info.getDefaultInstance())
            .build();
    }

    static final class FakeTransport implements Transport {

        record Sent(Address address, byte[] payload, MessageKind kind, CompletableFuture<byte[]> response) {
        }

        private final List<Sent> sent = new CopyOnWriteArrayList<>();

        @Override
        public void send(Address address, byte[] payload, MessageKind kind) {
            sent.add(new Sent(address, payload, kind, CompletableFuture.completedFuture(new byte[0])));
        }

        @Override
        public CompletableFuture<byte[]> sendAndReceive(Address address, byte[] payload, MessageKind kind) {
            var response = new CompletableFuture<byte[]>();
            sent.add(new Sent(address, payload, kind, response));
            return response;
        }

        Sent last(MessageKind kind) {
            return sent.stream()
                .filter(entry -> entry.kind() == kind)
                .reduce((first, second) -> second)
                .orElseThrow(() -> new AssertionError("no " + kind + " message sent"));
        }
    }
}
