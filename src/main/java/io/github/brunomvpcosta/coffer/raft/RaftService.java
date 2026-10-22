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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
    private static final long DEFAULT_SNAPSHOT_THRESHOLD = 1024;
    private static final long DISCOVERY_GRACE_MS = 3000;
    private static final long TICK_MS = 50;
    private static final long READ_TIMEOUT_SECONDS = 2;

    private final Node self;
    private final Transport transport;
    private final long snapshotThreshold;
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
    private final ExecutorService snapshotWriter = Executors.newSingleThreadExecutor(task -> {
        var thread = new Thread(task, "raft-snapshot");
        thread.setDaemon(true);
        return thread;
    });

    private final List<RaftProtocol.LogEntry> log = new ArrayList<>();
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<Long, CompletableFuture<Long>> applyWaiters = new HashMap<>();
    private final Map<Long, List<CompletableFuture<Void>>> appliedWaiters = new HashMap<>();
    private final List<ReadRequest> readRequests = new ArrayList<>();
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
    private long logBaseIndex;
    private long logBaseTerm;
    private long lastSnapshotIndex;
    private boolean snapshotInProgress;
    private RaftProtocol.Snapshot snapshot = RaftProtocol.Snapshot.getDefaultInstance();
    private RaftStateMachine stateMachine = new RaftStateMachine() {
        @Override
        public void apply(RaftProtocol.LogEntry entry) {
        }

        @Override
        public byte[] snapshot() {
            return new byte[0];
        }

        @Override
        public void restore(byte[] snapshot) {
        }
    };

    /**
     * Immutable Raft configuration (peers, excluding the local node). Passing a
     * roster that differs from another node's roster breaks quorum safety, so
     * every node must be given the same set of peers.
     */
    private final List<Node> configuration;

    public RaftService(Node self, Collection<Node> peers, Transport transport, RaftStorage storage) {
        this(self, peers, transport, storage, DEFAULT_SNAPSHOT_THRESHOLD,
            DEFAULT_HEARTBEAT_MS, DEFAULT_ELECTION_MIN_MS, DEFAULT_ELECTION_MAX_MS,
            System::currentTimeMillis, new Random());
    }

    public RaftService(
        Node self,
        Collection<Node> peers,
        Transport transport,
        RaftStorage storage,
        long snapshotThreshold) {

        this(self, peers, transport, storage, snapshotThreshold,
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

        this(self, peers, transport, storage, DEFAULT_SNAPSHOT_THRESHOLD,
            heartbeatMs, electionMinMs, electionMaxMs, clock, random);
    }

    public RaftService(
        Node self,
        Collection<Node> peers,
        Transport transport,
        RaftStorage storage,
        long snapshotThreshold,
        long heartbeatMs,
        long electionMinMs,
        long electionMaxMs,
        LongSupplier clock,
        Random random) {

        this.self = Objects.requireNonNull(self);
        this.transport = Objects.requireNonNull(transport);
        this.storage = Objects.requireNonNull(storage);
        this.snapshotThreshold = snapshotThreshold;
        this.heartbeatMs = heartbeatMs;
        this.electionMinMs = electionMinMs;
        this.electionMaxMs = electionMaxMs;
        this.clock = Objects.requireNonNull(clock);
        this.random = Objects.requireNonNull(random);
        this.configuration = peers.stream()
            .filter(node -> !node.id().equals(self.id()))
            .distinct()
            .toList();

        var loadedSnapshot = storage.loadSnapshot();

        if (loadedSnapshot != null) {
            this.snapshot = loadedSnapshot;
        }

        this.logBaseIndex = snapshot.getLastIncludedIndex();
        this.logBaseTerm = snapshot.getLastIncludedTerm();
        this.lastSnapshotIndex = logBaseIndex;
        this.commitIndex = logBaseIndex;
        this.lastApplied = logBaseIndex;

        var state = storage.load();
        this.currentTerm = state.getCurrentTerm();
        this.votedFor = state.getVotedFor().isEmpty() ? null : state.getVotedFor();

        for (var entry : state.getLogList()) {
            if (entry.getIndex() <= logBaseIndex) {
                continue;
            }

            if (entry.getIndex() != logBaseIndex + log.size() + 1) {
                throw new IllegalStateException("Corrupt Raft log: expected index "
                    + (logBaseIndex + log.size() + 1) + " but found " + entry.getIndex());
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
        snapshotWriter.shutdownNow();
    }

    /**
     * Registers a state machine that receives committed entries in order and
     * can snapshot and restore its state.
     */
    public synchronized void setStateMachine(RaftStateMachine stateMachine) {
        this.stateMachine = Objects.requireNonNull(stateMachine);

        if (snapshot.getLastIncludedIndex() > 0) {
            stateMachine.restore(snapshot.getData().toByteArray());
        }
    }

    /**
     * Convenience for tests and consumers that do not support snapshots.
     */
    public synchronized void setApplier(Consumer<RaftProtocol.LogEntry> applier) {
        var delegate = Objects.requireNonNull(applier);

        this.stateMachine = new RaftStateMachine() {
            @Override
            public void apply(RaftProtocol.LogEntry entry) {
                delegate.accept(entry);
            }

            @Override
            public byte[] snapshot() {
                return new byte[0];
            }

            @Override
            public void restore(byte[] snapshot) {
            }
        };
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
            case INSTALL_SNAPSHOT -> onInstallSnapshot(message.getInstallSnapshot());
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

        if (request.getPrevLogIndex() < logBaseIndex) {
            return RaftMessages.appendEntriesResponse(currentTerm, false, lastLogIndex());
        }

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

    private RaftProtocol.RaftMessage onInstallSnapshot(RaftProtocol.InstallSnapshot request) {
        if (request.getTerm() < currentTerm) {
            return RaftMessages.installSnapshotResponse(currentTerm, false, lastLogIndex());
        }

        if (request.getTerm() > currentTerm) {
            becomeFollower(request.getTerm());
        }

        role = Role.FOLLOWER;
        leaderId = request.getLeaderId();
        votesGranted.clear();
        resetElectionTimeout();

        var incoming = request.getSnapshot();

        // An older snapshot is harmless: we already have at least that state.
        if (incoming.getLastIncludedIndex() <= commitIndex) {
            return RaftMessages.installSnapshotResponse(currentTerm, true, lastLogIndex());
        }

        log.clear();
        logBaseIndex = incoming.getLastIncludedIndex();
        logBaseTerm = incoming.getLastIncludedTerm();
        commitIndex = logBaseIndex;
        lastApplied = logBaseIndex;
        lastSnapshotIndex = logBaseIndex;
        snapshot = incoming;

        storage.saveSnapshot(incoming);
        stateMachine.restore(incoming.getData().toByteArray());
        persist();
        completeAppliedWaiters();

        log("installed snapshot at index " + logBaseIndex);

        return RaftMessages.installSnapshotResponse(currentTerm, true, logBaseIndex);
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
            acknowledgeReads(peer.id());
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

        for (var request : readRequests) {
            request.future.completeExceptionally(new RaftNotLeaderException(leaderId));
        }

        readRequests.clear();

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

        if (next <= logBaseIndex) {
            sendInstallSnapshot(peer);
            return;
        }

        var previous = next - 1;
        var entries = new ArrayList<RaftProtocol.LogEntry>();

        for (long index = next; index <= lastLogIndex(); index++) {
            entries.add(entryAt(index));
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

    private void sendInstallSnapshot(Node peer) {
        if (snapshot.getLastIncludedIndex() == 0) {
            return;
        }

        var request = RaftMessages.installSnapshot(currentTerm, selfId(), snapshot);

        transport.sendAndReceive(address(peer), request.toByteArray(), MessageKind.RAFT)
            .whenComplete((payload, cause) -> {
                if (cause != null) {
                    return;
                }

                try {
                    var response = RaftProtocol.RaftMessage.parseFrom(payload);
                    if (response.hasInstallSnapshotResponse()) {
                        onInstallSnapshotResponse(peer, response.getInstallSnapshotResponse());
                    }
                } catch (InvalidProtocolBufferException ex) {
                    log("invalid snapshot response: " + ex.getMessage());
                }
            });
    }

    private synchronized void onInstallSnapshotResponse(Node peer, RaftProtocol.InstallSnapshotResponse response) {
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

        // The follower may already be ahead of the snapshot it rejected.
        if (response.getMatchIndex() > 0) {
            matchIndex.merge(peer.id(), response.getMatchIndex(), Math::max);
            nextIndex.put(peer.id(), matchIndex.getOrDefault(peer.id(), 0L) + 1);
            sendAppendEntries(peer);
        }
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
            var entry = entryAt(lastApplied);
            RuntimeException failure = null;

            try {
                stateMachine.apply(entry);
            } catch (RuntimeException ex) {
                failure = ex;
                log("state machine failed at index " + entry.getIndex() + ": " + ex);
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

            if (failure == null) {
                maybeSnapshot();
            }
        }

        completeAppliedWaiters();
    }

    private void maybeSnapshot() {
        if (snapshotThreshold <= 0 || snapshotInProgress
            || lastApplied - lastSnapshotIndex < snapshotThreshold) {
            return;
        }

        var data = stateMachine.snapshot();

        if (data.length == 0) {
            return;
        }

        var index = lastApplied;
        var next = RaftProtocol.Snapshot.newBuilder()
            .setLastIncludedIndex(index)
            .setLastIncludedTerm(termAt(index))
            .setData(com.google.protobuf.ByteString.copyFrom(data))
            .build();

        snapshotInProgress = true;

        try {
            snapshotWriter.execute(() -> writeSnapshot(next));
        } catch (RejectedExecutionException ex) {
            snapshotInProgress = false;
            log("snapshot rejected: " + ex.getMessage());
        }
    }

    private void writeSnapshot(RaftProtocol.Snapshot next) {
        try {
            storage.saveSnapshot(next);
        } catch (RuntimeException ex) {
            log("snapshot write failed: " + ex);
            snapshotWriteFinished(null);
            return;
        }

        snapshotWriteFinished(next);
    }

    private synchronized void snapshotWriteFinished(RaftProtocol.Snapshot saved) {
        snapshotInProgress = false;

        if (saved == null || saved.getLastIncludedIndex() <= logBaseIndex) {
            return;
        }

        snapshot = saved;
        compactTo(saved.getLastIncludedIndex(), saved.getLastIncludedTerm());
        lastSnapshotIndex = saved.getLastIncludedIndex();
        persist();

        log("snapshot at index " + saved.getLastIncludedIndex() + ", log entries: " + log.size());
    }

    private void compactTo(long index, long term) {
        var remove = (int) Math.min(log.size(), Math.max(0, index - logBaseIndex));

        if (remove > 0) {
            log.subList(0, remove).clear();
        }

        logBaseIndex = index;
        logBaseTerm = term;
    }

    private void truncateFrom(long index) {
        if (index <= logBaseIndex) {
            return;
        }

        var offset = (int) (index - logBaseIndex - 1);

        while (log.size() > offset) {
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
        return logBaseIndex + log.size();
    }

    private long lastLogTerm() {
        return log.isEmpty() ? logBaseTerm : log.get(log.size() - 1).getTerm();
    }

    private long termAt(long index) {
        if (index == logBaseIndex) {
            return logBaseTerm;
        }

        if (index < logBaseIndex) {
            return 0;
        }

        var offset = index - logBaseIndex - 1;

        if (offset < 0 || offset >= log.size()) {
            return 0;
        }

        return log.get((int) offset).getTerm();
    }

    private RaftProtocol.LogEntry entryAt(long index) {
        var offset = index - logBaseIndex - 1;

        if (offset < 0 || offset >= log.size()) {
            throw new IllegalStateException("Log entry " + index + " is not available (base "
                + logBaseIndex + ", size " + log.size() + ")");
        }

        return log.get((int) offset);
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

    /**
     * Confirms leadership with a heartbeat round and returns the commit index
     * that must be applied before serving a linearizable read.
     */
    public synchronized CompletableFuture<Long> readIndex() {
        if (role != Role.LEADER) {
            return CompletableFuture.failedFuture(new RaftNotLeaderException(leaderId));
        }

        var request = new ReadRequest(commitIndex, selfId());

        if (request.acknowledged.size() >= quorum()) {
            return CompletableFuture.completedFuture(request.index);
        }

        readRequests.add(request);
        sendHeartbeats();

        return request.future
            .orTimeout(READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .whenComplete((index, cause) -> {
                synchronized (RaftService.this) {
                    readRequests.remove(request);
                }
            });
    }

    /**
     * Completes once the local state machine has applied {@code index}.
     */
    public synchronized CompletableFuture<Void> awaitApplied(long index) {
        if (lastApplied >= index) {
            return CompletableFuture.completedFuture(null);
        }

        var future = new CompletableFuture<Void>();
        appliedWaiters.computeIfAbsent(index, key -> new ArrayList<>()).add(future);
        return future;
    }

    private void acknowledgeReads(String peerId) {
        var iterator = readRequests.iterator();

        while (iterator.hasNext()) {
            var request = iterator.next();
            request.acknowledged.add(peerId);

            if (request.acknowledged.size() >= quorum()) {
                request.future.complete(request.index);
                iterator.remove();
            }
        }
    }

    private void completeAppliedWaiters() {
        var iterator = appliedWaiters.entrySet().iterator();

        while (iterator.hasNext()) {
            var entry = iterator.next();

            if (entry.getKey() <= lastApplied) {
                entry.getValue().forEach(future -> future.complete(null));
                iterator.remove();
            }
        }
    }

    private static final class ReadRequest {

        private final long index;
        private final Set<String> acknowledged = new HashSet<>();
        private final CompletableFuture<Long> future = new CompletableFuture<>();

        private ReadRequest(long index, String selfId) {
            this.index = index;
            this.acknowledged.add(selfId);
        }
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
        return log.size();
    }

    public synchronized long lastIndex() {
        return lastLogIndex();
    }

    public synchronized long snapshotIndex() {
        return snapshot.getLastIncludedIndex();
    }

    public synchronized long logLastTerm() {
        return lastLogTerm();
    }
}
