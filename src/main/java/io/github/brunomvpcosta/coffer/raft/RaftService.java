package io.github.brunomvpcosta.coffer.raft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.brunomvpcosta.coffer.cluster.Node;
import io.github.brunomvpcosta.coffer.net.Address;
import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;
import io.github.brunomvpcosta.coffer.transport.MessageKind;
import io.github.brunomvpcosta.coffer.transport.Transport;

/**
 * A Raft node.
 *
 * <p>This class implements leader election, heartbeats and log replication on
 * top of {@link Transport}. The Raft configuration is supplied explicitly
 * (normally the seed roster) and never changes; Raft never derives quorum from
 * live discovery. All state is guarded by the intrinsic lock so that timer
 * ticks and network callbacks can share it.</p>
 *
 * <p>The log is 1-based (index {@code i} lives at list position {@code i - 1}).
 * Index 0 means "empty" and has term 0.</p>
 */
public class RaftService implements RaftMessageHandler {

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private static final long DEFAULT_HEARTBEAT_MS = 300;
    private static final long DEFAULT_ELECTION_MIN_MS = 1000;
    private static final long DEFAULT_ELECTION_MAX_MS = 2000;
    private static final long DISCOVERY_GRACE_MS = 3000;
    private static final long TICK_MS = 50;

    private final Node self;
    private final Transport transport;
    private final long heartbeatMs;
    private final long electionMinMs;
    private final long electionMaxMs;
    private final LongSupplier clock;
    private final Random random;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
        var thread = new Thread(task, "raft-ticker");
        thread.setDaemon(true);
        return thread;
    });

    private final List<RaftProtocol.LogEntry> log = new ArrayList<>();
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<Long, CompletableFuture<Long>> applyWaiters = new HashMap<>();
    private final Set<String> votesGranted = new HashSet<>();
    private final RaftStorage storage;

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private String votedFor;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadlineMs;
    private long nextHeartbeatMs;
    private Consumer<RaftProtocol.LogEntry> applier = entry -> { };

    /**
     * Immutable Raft configuration (peers, excluding the local node). Passing a
     * roster that differs from another node's roster breaks quorum safety, so
     * every node must be given the same set of peers.
     */
    private final List<Node> configuration;

    public RaftService(Node self, Collection<Node> peers, Transport transport, RaftStorage storage) {
        this(self, peers, transport, storage,
            DEFAULT_HEARTBEAT_MS, DEFAULT_ELECTION_MIN_MS, DEFAULT_ELECTION_MAX_MS,
            System::currentTimeMillis, new Random());
    }

    public RaftService(
        Node self,
        Collection<Node> peers,
        Transport transport,
        RaftStorage storage,
        long heartbeatMs,
        long electionMinMs,
        long electionMaxMs,
        LongSupplier clock,
        Random random) {

        this.self = Objects.requireNonNull(self);
        this.transport = Objects.requireNonNull(transport);
        this.storage = Objects.requireNonNull(storage);
        this.heartbeatMs = heartbeatMs;
        this.electionMinMs = electionMinMs;
        this.electionMaxMs = electionMaxMs;
        this.clock = Objects.requireNonNull(clock);
        this.random = Objects.requireNonNull(random);
        this.configuration = peers.stream()
            .filter(node -> !node.id().equals(self.id()))
            .distinct()
            .toList();

        var state = storage.load();
        this.currentTerm = state.getCurrentTerm();
        this.votedFor = state.getVotedFor().isEmpty() ? null : state.getVotedFor();

        for (var entry : state.getLogList()) {
            if (entry.getIndex() != log.size() + 1) {
                throw new IllegalStateException("Corrupt Raft log: expected index "
                    + (log.size() + 1) + " but found " + entry.getIndex());
            }
            log.add(entry);
        }

        this.currentTerm = Math.max(this.currentTerm, lastLogTerm());
        this.electionDeadlineMs = clock.getAsLong() + DISCOVERY_GRACE_MS + electionTimeoutMs();
    }

    public synchronized void start() {
        log("configuration: " + configuration.stream().map(Node::id).toList());
        scheduler.scheduleWithFixedDelay(this::tickSafely, TICK_MS, TICK_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    /**
     * Registers the state machine that receives committed entries in order.
     */
    public synchronized void setApplier(Consumer<RaftProtocol.LogEntry> applier) {
        this.applier = Objects.requireNonNull(applier);
    }

    /**
     * Appends a command to the leader's log and starts replicating it.
     *
     * @return a future completed with the log index once the entry is applied
     * @throws IllegalStateException when this node is not the leader
     */
    public synchronized CompletableFuture<Long> append(byte[] command) {
        if (role != Role.LEADER) {
            throw new IllegalStateException("not the leader: " + role);
        }

        var index = lastLogIndex() + 1;
        var applied = new CompletableFuture<Long>();
        applyWaiters.put(index, applied);
        log.add(RaftMessages.entry(index, currentTerm, command));
        persist();

        advanceCommitIndex();
        sendHeartbeats();

        return applied;
    }

    @Override
    public synchronized RaftProtocol.RaftMessage handle(RaftProtocol.RaftMessage message) {
        return switch (message.getPayloadCase()) {
            case REQUEST_VOTE -> onRequestVote(message.getRequestVote());
            case APPEND_ENTRIES -> onAppendEntries(message.getAppendEntries());
            default -> null;
        };
    }

    /**
     * Drives timeouts and leader heartbeats. Called by the scheduler in
     * production and directly by tests.
     */
    public synchronized void tick() {
        var now = clock.getAsLong();

        if (role == Role.LEADER) {
            if (now >= nextHeartbeatMs) {
                sendHeartbeats();
            }
            return;
        }

        if (now >= electionDeadlineMs) {
            startElection();
        }
    }

    private void tickSafely() {
        try {
            tick();
        } catch (Exception ex) {
            log("tick failed: " + ex);
        }
    }

    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = selfId();
        leaderId = null;
        votesGranted.clear();
        votesGranted.add(selfId());
        resetElectionTimeout();
        persist();

        log("starting election for term " + currentTerm);

        if (votesGranted.size() >= quorum()) {
            becomeLeader();
            return;
        }

        var request = RaftMessages.requestVote(currentTerm, selfId(), lastLogIndex(), lastLogTerm());

        for (var peer : peers()) {
            transport.sendAndReceive(address(peer), request.toByteArray(), MessageKind.RAFT)
                .whenComplete((payload, cause) -> {
                    if (cause != null) {
                        return;
                    }

                    try {
                        var response = RaftProtocol.RaftMessage.parseFrom(payload);
                        if (response.hasRequestVoteResponse()) {
                            onRequestVoteResponse(peer, response.getRequestVoteResponse());
                        }
                    } catch (InvalidProtocolBufferException ex) {
                        log("invalid vote response: " + ex.getMessage());
                    }
                });
        }
    }

    private synchronized void onRequestVoteResponse(Node peer, RaftProtocol.RequestVoteResponse response) {
        if (response.getTerm() > currentTerm) {
            becomeFollower(response.getTerm());
            return;
        }

        if (role != Role.CANDIDATE || response.getTerm() < currentTerm || !response.getVoteGranted()) {
            return;
        }

        votesGranted.add(peer.id());

        if (votesGranted.size() >= quorum()) {
            becomeLeader();
        }
    }

    private RaftProtocol.RaftMessage onRequestVote(RaftProtocol.RequestVote request) {
        if (request.getTerm() < currentTerm) {
            return RaftMessages.requestVoteResponse(currentTerm, false);
        }

        if (request.getTerm() > currentTerm) {
            becomeFollower(request.getTerm());
        }

        var upToDate = request.getLastLogTerm() > lastLogTerm()
            || (request.getLastLogTerm() == lastLogTerm() && request.getLastLogIndex() >= lastLogIndex());

        var canVote = (votedFor == null || votedFor.equals(request.getCandidateId())) && upToDate;

        if (!canVote) {
            return RaftMessages.requestVoteResponse(currentTerm, false);
        }

        votedFor = request.getCandidateId();
        resetElectionTimeout();
        persist();
        log("voted for " + request.getCandidateId() + " in term " + currentTerm);

        return RaftMessages.requestVoteResponse(currentTerm, true);
    }

    private RaftProtocol.RaftMessage onAppendEntries(RaftProtocol.AppendEntries request) {
        if (request.getTerm() < currentTerm) {
            return RaftMessages.appendEntriesResponse(currentTerm, false, lastLogIndex());
        }

        if (request.getTerm() > currentTerm) {
            becomeFollower(request.getTerm());
        }

        role = Role.FOLLOWER;
        leaderId = request.getLeaderId();
        votesGranted.clear();
        resetElectionTimeout();

        if (request.getPrevLogIndex() > lastLogIndex()
            || termAt(request.getPrevLogIndex()) != request.getPrevLogTerm()) {
            return RaftMessages.appendEntriesResponse(currentTerm, false, lastLogIndex());
        }

        var logChanged = false;

        for (var entry : request.getEntriesList()) {
            var index = entry.getIndex();

            if (index <= lastLogIndex()) {
                if (termAt(index) == entry.getTerm()) {
                    continue;
                }

                truncateFrom(index);
                logChanged = true;
            }

            if (index != lastLogIndex() + 1) {
                return RaftMessages.appendEntriesResponse(currentTerm, false, lastLogIndex());
            }

            log.add(entry);
            logChanged = true;
        }

        if (request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), lastLogIndex());
            applyCommitted();
        }

        if (logChanged) {
            persist();
        }

        return RaftMessages.appendEntriesResponse(currentTerm, true, lastLogIndex());
    }

    private synchronized void onAppendEntriesResponse(Node peer, RaftProtocol.AppendEntriesResponse response) {
        if (response.getTerm() > currentTerm) {
            becomeFollower(response.getTerm());
            return;
        }

        if (role != Role.LEADER || response.getTerm() < currentTerm) {
            return;
        }

        if (response.getSuccess()) {
            matchIndex.merge(peer.id(), response.getMatchIndex(), Math::max);
            nextIndex.put(peer.id(), matchIndex.getOrDefault(peer.id(), 0L) + 1);
            advanceCommitIndex();
            return;
        }

        var next = nextIndex.getOrDefault(peer.id(), lastLogIndex() + 1);
        nextIndex.put(peer.id(), Math.max(1, next - 1));
        sendAppendEntries(peer);
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = selfId();
        votesGranted.clear();
        nextIndex.clear();
        matchIndex.clear();

        var next = lastLogIndex() + 1;

        for (var peer : peers()) {
            nextIndex.put(peer.id(), next);
            matchIndex.put(peer.id(), 0L);
        }

        // A no-op in the new term lets the leader commit entries left over from
        // previous terms and proves its authority to the rest of the cluster.
        log.add(RaftMessages.entry(next, currentTerm, new byte[0]));
        persist();

        nextHeartbeatMs = 0;
        log("elected leader for term " + currentTerm);
        advanceCommitIndex();
        sendHeartbeats();
    }

    private void becomeFollower(long term) {
        var wasLeader = role == Role.LEADER;

        role = Role.FOLLOWER;

        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            persist();
        }

        leaderId = null;
        votesGranted.clear();
        resetElectionTimeout();

        if (wasLeader) {
            log("stepped down in term " + currentTerm);
        }
    }

    private void sendHeartbeats() {
        nextHeartbeatMs = clock.getAsLong() + heartbeatMs;

        for (var peer : peers()) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(Node peer) {
        var next = nextIndex.getOrDefault(peer.id(), lastLogIndex() + 1);
        var previous = Math.max(0, next - 1);
        var entries = new ArrayList<RaftProtocol.LogEntry>();

        for (long index = next; index <= lastLogIndex(); index++) {
            entries.add(log.get((int) index - 1));
        }

        var request = RaftMessages.appendEntries(
            currentTerm, selfId(), previous, termAt(previous), entries, commitIndex);

        transport.sendAndReceive(address(peer), request.toByteArray(), MessageKind.RAFT)
            .whenComplete((payload, cause) -> {
                if (cause != null) {
                    return;
                }

                try {
                    var response = RaftProtocol.RaftMessage.parseFrom(payload);
                    if (response.hasAppendEntriesResponse()) {
                        onAppendEntriesResponse(peer, response.getAppendEntriesResponse());
                    }
                } catch (InvalidProtocolBufferException ex) {
                    log("invalid append response: " + ex.getMessage());
                }
            });
    }

    private void advanceCommitIndex() {
        for (var candidate = lastLogIndex(); candidate > commitIndex; candidate--) {
            if (termAt(candidate) != currentTerm) {
                continue;
            }

            var replicated = 1;

            for (var peer : peers()) {
                if (matchIndex.getOrDefault(peer.id(), 0L) >= candidate) {
                    replicated++;
                }
            }

            if (replicated >= quorum()) {
                commitIndex = candidate;
                applyCommitted();
                return;
            }
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            var entry = log.get((int) lastApplied - 1);
            RuntimeException failure = null;

            try {
                applier.accept(entry);
            } catch (RuntimeException ex) {
                failure = ex;
                log("applier failed at index " + entry.getIndex() + ": " + ex);
            } finally {
                var waiter = applyWaiters.remove(entry.getIndex());

                if (waiter != null) {
                    if (failure != null) {
                        waiter.completeExceptionally(failure);
                    } else {
                        waiter.complete(entry.getIndex());
                    }
                }
            }
        }
    }

    private void truncateFrom(long index) {
        while (lastLogIndex() >= index) {
            var removed = log.remove(log.size() - 1);
            var waiter = applyWaiters.remove(removed.getIndex());

            if (waiter != null) {
                waiter.completeExceptionally(
                    new IllegalStateException("Log entry " + removed.getIndex() + " was truncated"));
            }
        }
    }

    private void persist() {
        storage.save(RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(currentTerm)
            .setVotedFor(votedFor == null ? "" : votedFor)
            .addAllLog(log)
            .build());
    }

    private long lastLogIndex() {
        return log.size();
    }

    private long lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    private long termAt(long index) {
        if (index <= 0 || index > lastLogIndex()) {
            return 0;
        }

        return log.get((int) index - 1).getTerm();
    }

    private int quorum() {
        return (configuration.size() + 1) / 2 + 1;
    }

    private List<Node> peers() {
        return configuration;
    }

    private String selfId() {
        return self.id();
    }

    private void resetElectionTimeout() {
        electionDeadlineMs = clock.getAsLong() + electionTimeoutMs();
    }

    private long electionTimeoutMs() {
        var spread = (int) Math.max(1, electionMaxMs - electionMinMs + 1);
        return electionMinMs + random.nextInt(spread);
    }

    private Address address(Node node) {
        return new Address(node.host(), node.port());
    }

    private void log(String message) {
        System.out.println("[RAFT] - " + selfId() + " " + message);
    }

    public synchronized Role role() {
        return role;
    }

    public synchronized Node self() {
        return self;
    }

    public synchronized Node leader() {
        if (leaderId == null) {
            return null;
        }

        if (leaderId.equals(self.id())) {
            return self;
        }

        return configuration.stream()
            .filter(node -> node.id().equals(leaderId))
            .findFirst()
            .orElse(null);
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized String leaderId() {
        return leaderId;
    }

    public synchronized boolean isLeader() {
        return role == Role.LEADER;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public synchronized long logSize() {
        return lastLogIndex();
    }

    public synchronized long logLastTerm() {
        return lastLogTerm();
    }
}
