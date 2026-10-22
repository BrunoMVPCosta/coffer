package io.github.brunomvpcosta.coffer.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import io.github.brunomvpcosta.coffer.cluster.Node;
import io.github.brunomvpcosta.coffer.net.Address;
import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;
import io.github.brunomvpcosta.coffer.raft.RaftService.Role;
import io.github.brunomvpcosta.coffer.transport.MessageKind;
import io.github.brunomvpcosta.coffer.transport.Transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RaftServiceTest {

    private static final Node LOCAL = new Node("n1", "127.0.0.1", 8080);
    private static final Node PEER2 = new Node("n2", "127.0.0.1", 8081);
    private static final Node PEER3 = new Node("n3", "127.0.0.1", 8082);

    private final FakeTransport transport = new FakeTransport();
    private final AtomicLong clock = new AtomicLong();
    private final InMemoryRaftStorage storage = new InMemoryRaftStorage();
    private RaftService raft = newRaft();

    @AfterEach
    void stop() {
        raft.stop();
    }

    @Test
    void singleNodeBecomesLeaderAfterElectionTimeout() {
        afterDiscovery();

        raft.tick();

        assertTrue(raft.isLeader());
        assertEquals(1, raft.currentTerm());
        assertEquals(LOCAL.id(), raft.leaderId());
        assertEquals(1, raft.logSize());
        assertEquals(1, raft.commitIndex());
    }

    @Test
    void candidateBecomesLeaderAfterWinningQuorum() {
        raft = newRaft(PEER2, PEER3);
        afterDiscovery();

        raft.tick();

        assertEquals(Role.CANDIDATE, raft.role());
        assertEquals(2, transport.lastRequests().size());

        transport.completeVote(PEER2, 1, true);

        assertTrue(raft.isLeader());
        assertEquals(1, raft.currentTerm());
    }

    @Test
    void candidateIgnoresRejectedVotes() {
        raft = newRaft(PEER2, PEER3);
        afterDiscovery();

        raft.tick();
        transport.completeVote(PEER2, 1, false);
        transport.completeVote(PEER3, 1, false);

        assertEquals(Role.CANDIDATE, raft.role());
    }

    @Test
    void staleVoteRequestsAreRejected() {
        var granted = raft.handle(RaftMessages.requestVote(5, PEER2.id(), 0, 0));
        var stale = raft.handle(RaftMessages.requestVote(4, PEER3.id(), 0, 0));

        assertTrue(granted.getRequestVoteResponse().getVoteGranted());
        assertFalse(stale.getRequestVoteResponse().getVoteGranted());
        assertEquals(5, raft.currentTerm());
    }

    @Test
    void voteIsDeniedWhenCandidateLogIsBehind() {
        raft.handle(RaftMessages.appendEntries(1, PEER2.id(), 0, 0,
            List.of(RaftMessages.entry(1, 1, bytes("a"))), 0));

        var behind = raft.handle(RaftMessages.requestVote(2, PEER3.id(), 0, 0));

        assertFalse(behind.getRequestVoteResponse().getVoteGranted());
    }

    @Test
    void voteIsGrantedWhenCandidateLogIsUpToDate() {
        raft.handle(RaftMessages.appendEntries(1, PEER2.id(), 0, 0,
            List.of(RaftMessages.entry(1, 1, bytes("a"))), 0));

        var response = raft.handle(RaftMessages.requestVote(2, PEER3.id(), 1, 1));

        assertTrue(response.getRequestVoteResponse().getVoteGranted());
    }

    @Test
    void higherTermStepsLeaderDown() {
        afterDiscovery();
        raft.tick();
        assertTrue(raft.isLeader());

        var response = raft.handle(RaftMessages.appendEntries(2, PEER2.id(), 0, 0, List.of(), 0));

        assertEquals(Role.FOLLOWER, raft.role());
        assertEquals(2, raft.currentTerm());
        assertEquals(PEER2.id(), raft.leaderId());
        assertTrue(response.getAppendEntriesResponse().getSuccess());
    }

    @Test
    void followerRejectsAppendEntriesWithMissingPreviousEntry() {
        var response = raft.handle(RaftMessages.appendEntries(1, PEER2.id(), 3, 1, List.of(), 0));

        assertFalse(response.getAppendEntriesResponse().getSuccess());
    }

    @Test
    void followerAppendsAndCommitsEntries() {
        var applied = new ArrayList<RaftProtocol.LogEntry>();
        raft.setApplier(applied::add);

        var response = raft.handle(RaftMessages.appendEntries(1, PEER2.id(), 0, 0,
            List.of(RaftMessages.entry(1, 1, bytes("a")), RaftMessages.entry(2, 1, bytes("b"))), 1));

        assertTrue(response.getAppendEntriesResponse().getSuccess());
        assertEquals(2, raft.logSize());
        assertEquals(1, raft.commitIndex());
        assertEquals(List.of("a"), applied.stream().map(entry -> entry.getCommand().toStringUtf8()).toList());
    }

    @Test
    void leaderReplicatesEntryAndCommitsOnMajority() throws Exception {
        raft = newRaft(PEER2, PEER3);
        afterDiscovery();
        raft.tick();
        transport.completeVote(PEER2, 1, true);
        assertTrue(raft.isLeader());

        var applied = new ArrayList<RaftProtocol.LogEntry>();
        raft.setApplier(applied::add);

        var appliedFuture = raft.append(bytes("set a=1"));

        assertEquals(0, raft.commitIndex());
        assertFalse(appliedFuture.isDone());

        transport.completeAppend(PEER2, 1, true, 2);

        assertEquals(2, raft.commitIndex());
        assertEquals(2, applied.size());
        assertEquals("", applied.get(0).getCommand().toStringUtf8());
        assertEquals("set a=1", applied.get(1).getCommand().toStringUtf8());
        assertEquals(2L, appliedFuture.get());
    }

    @Test
    void appendFailsWhenNotLeader() {
        assertThrows(IllegalStateException.class, () -> raft.append(bytes("a")));
    }

    @Test
    void termAndVoteSurviveRestart() {
        var granted = raft.handle(RaftMessages.requestVote(5, PEER2.id(), 0, 0));
        assertTrue(granted.getRequestVoteResponse().getVoteGranted());

        raft = newRaft();

        assertEquals(5, raft.currentTerm());

        var denied = raft.handle(RaftMessages.requestVote(5, PEER3.id(), 0, 0));

        assertFalse(denied.getRequestVoteResponse().getVoteGranted());
    }

    @Test
    void logSurvivesRestart() {
        raft.handle(RaftMessages.appendEntries(1, PEER2.id(), 0, 0,
            List.of(RaftMessages.entry(1, 1, bytes("a")), RaftMessages.entry(2, 1, bytes("b"))), 0));

        raft = newRaft();

        assertEquals(2, raft.logSize());
        assertEquals(1, raft.logLastTerm());
    }

    @Test
    void candidateCannotWinWithOnlyOnePeerInFiveNodeCluster() {
        var peer4 = new Node("n4", "127.0.0.1", 8083);
        var peer5 = new Node("n5", "127.0.0.1", 8084);
        raft = newRaft(PEER2, PEER3, peer4, peer5);
        afterDiscovery();

        raft.tick();
        assertEquals(Role.CANDIDATE, raft.role());
        assertEquals(4, transport.lastRequests().size());

        transport.completeVote(PEER2, 1, true);
        assertEquals(Role.CANDIDATE, raft.role());

        transport.completeVote(PEER3, 1, true);
        assertTrue(raft.isLeader());
    }

    private RaftService newRaft(Node... peers) {
        return new RaftService(
            LOCAL, List.of(peers), transport, storage, 50, 100, 200, clock::get, new Random(7));
    }

    private void afterDiscovery() {
        clock.set(10_000);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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

        List<Sent> lastRequests() {
            return sent.stream()
                .filter(entry -> entry.kind() == MessageKind.RAFT)
                .toList();
        }

        void completeVote(Node peer, long term, boolean granted) {
            lastTo(peer).response().complete(
                RaftMessages.requestVoteResponse(term, granted).toByteArray());
        }

        void completeAppend(Node peer, long term, boolean success, long matchIndex) {
            lastTo(peer).response().complete(
                RaftMessages.appendEntriesResponse(term, success, matchIndex).toByteArray());
        }

        private Sent lastTo(Node peer) {
            return lastRequests().stream()
                .filter(entry -> entry.address().equals(new Address(peer.host(), peer.port())))
                .reduce((first, second) -> second)
                .orElseThrow(() -> new AssertionError("no request sent to " + peer.id()));
        }
    }
}
