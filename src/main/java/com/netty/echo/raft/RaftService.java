package com.netty.echo.raft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import com.google.protobuf.InvalidProtocolBufferException;
import com.netty.echo.cluster.Cluster;
import com.netty.echo.cluster.Node;
import com.netty.echo.net.Address;
import com.netty.echo.protocol.RaftProtocol;
import com.netty.echo.transport.MessageKind;
import com.netty.echo.transport.Transport;

/**
 * A Raft node.
 *
 * <p>This class implements leader election, heartbeats and log replication on
 * top of {@link Transport}. Membership comes from {@link Cluster}; Raft never
 * changes the cluster configuration itself. All state is guarded by the
 * intrinsic lock so that timer ticks and network callbacks can share it.</p>
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

    private final Cluster cluster;
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
    private final Set<String> votesGranted = new HashSet<>();

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private String votedFor;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadlineMs;
    private long nextHeartbeatMs;
    private Consumer<RaftProtocol.LogEntry> applier = entry -> { };

    public RaftService(Cluster cluster, Transport transport) {
        this(cluster, transport,
            DEFAULT_HEARTBEAT_MS, DEFAULT_ELECTION_MIN_MS, DEFAULT_ELECTION_MAX_MS,
            System::currentTimeMillis, new Random());
    }

    RaftService(
        Cluster cluster,
        Transport transport,
        long heartbeatMs,
        long electionMinMs,
        long electionMaxMs,
        LongSupplier clock,
        Random random) {

        this.cluster = Objects.requireNonNull(cluster);
        this.transport = Objects.requireNonNull(transport);
        this.heartbeatMs = heartbeatMs;
        this.electionMinMs = electionMinMs;
        this.electionMaxMs = electionMaxMs;
        this.clock = Objects.requireNonNull(clock);
        this.random = Objects.requireNonNull(random);

        this.electionDeadlineMs = clock.getAsLong() + DISCOVERY_GRACE_MS + electionTimeoutMs();
    }

    public synchronized void start() {
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
     * @throws IllegalStateException when this node is not the leader
     */
    public synchronized long append(byte[] command) {
        if (role != Role.LEADER) {
            throw new IllegalStateException("not the leader: " + role);
        }

        var index = lastLogIndex() + 1;
        log.add(RaftMessages.entry(index, currentTerm, command));

        advanceCommitIndex();
        sendHeartbeats();

        return index;
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
    synchronized void tick() {
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

        for (var entry : request.getEntriesList()) {
            var index = entry.getIndex();

            if (index <= lastLogIndex()) {
                if (termAt(index) == entry.getTerm()) {
                    continue;
                }

                truncateFrom(index);
            }

            if (index != lastLogIndex() + 1) {
                return RaftMessages.appendEntriesResponse(currentTerm, false, lastLogIndex());
            }

            log.add(entry);
        }

        if (request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), lastLogIndex());
            applyCommitted();
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

        for (var peer : peers()) {
            nextIndex.put(peer.id(), lastLogIndex() + 1);
            matchIndex.put(peer.id(), 0L);
        }

        nextHeartbeatMs = 0;
        log("elected leader for term " + currentTerm);
        sendHeartbeats();
    }

    private void becomeFollower(long term) {
        var wasLeader = role == Role.LEADER;

        role = Role.FOLLOWER;

        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
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
            applier.accept(log.get((int) lastApplied - 1));
        }
    }

    private void truncateFrom(long index) {
        while (lastLogIndex() >= index) {
            log.remove(log.size() - 1);
        }
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
        return (cluster.getMembers().size() + 1) / 2 + 1;
    }

    private Collection<Node> peers() {
        return cluster.getMembers();
    }

    private String selfId() {
        return cluster.getMember().id();
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
