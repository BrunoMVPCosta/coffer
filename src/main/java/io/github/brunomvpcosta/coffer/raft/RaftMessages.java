package io.github.brunomvpcosta.coffer.raft;

import java.util.Collection;

import com.google.protobuf.ByteString;
import io.github.brunomvpcosta.coffer.cluster.Node;
import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

public final class RaftMessages {

    private RaftMessages() {
    }

    public static RaftProtocol.RaftMessage requestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        return RaftProtocol.RaftMessage.newBuilder()
            .setRequestVote(RaftProtocol.RequestVote.newBuilder()
                .setTerm(term)
                .setCandidateId(candidateId)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm))
            .build();
    }

    public static RaftProtocol.RaftMessage requestVoteResponse(long term, boolean granted) {
        return RaftProtocol.RaftMessage.newBuilder()
            .setRequestVoteResponse(RaftProtocol.RequestVoteResponse.newBuilder()
                .setTerm(term)
                .setVoteGranted(granted))
            .build();
    }

    public static RaftProtocol.RaftMessage appendEntries(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        Collection<RaftProtocol.LogEntry> entries,
        long leaderCommit) {

        return RaftProtocol.RaftMessage.newBuilder()
            .setAppendEntries(RaftProtocol.AppendEntries.newBuilder()
                .setTerm(term)
                .setLeaderId(leaderId)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .addAllEntries(entries)
                .setLeaderCommit(leaderCommit))
            .build();
    }

    public static RaftProtocol.RaftMessage appendEntriesResponse(long term, boolean success, long matchIndex) {
        return RaftProtocol.RaftMessage.newBuilder()
            .setAppendEntriesResponse(RaftProtocol.AppendEntriesResponse.newBuilder()
                .setTerm(term)
                .setSuccess(success)
                .setMatchIndex(matchIndex))
            .build();
    }

    public static RaftProtocol.RaftMessage installSnapshot(long term, String leaderId, RaftProtocol.Snapshot snapshot) {
        return RaftProtocol.RaftMessage.newBuilder()
            .setInstallSnapshot(RaftProtocol.InstallSnapshot.newBuilder()
                .setTerm(term)
                .setLeaderId(leaderId)
                .setSnapshot(snapshot))
            .build();
    }

    public static RaftProtocol.RaftMessage installSnapshotResponse(long term, boolean success, long matchIndex) {
        return RaftProtocol.RaftMessage.newBuilder()
            .setInstallSnapshotResponse(RaftProtocol.InstallSnapshotResponse.newBuilder()
                .setTerm(term)
                .setSuccess(success)
                .setMatchIndex(matchIndex))
            .build();
    }

    public static RaftProtocol.Snapshot snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
        return RaftProtocol.Snapshot.newBuilder()
            .setLastIncludedIndex(lastIncludedIndex)
            .setLastIncludedTerm(lastIncludedTerm)
            .setData(ByteString.copyFrom(data))
            .build();
    }

    public static RaftProtocol.LogEntry entry(long index, long term, byte[] command) {
        return RaftProtocol.LogEntry.newBuilder()
            .setIndex(index)
            .setTerm(term)
            .setCommand(ByteString.copyFrom(command))
            .setKind(RaftProtocol.EntryKind.ENTRY_COMMAND)
            .build();
    }

    public static RaftProtocol.LogEntry configurationEntry(long index, long term, Collection<Node> nodes) {
        return RaftProtocol.LogEntry.newBuilder()
            .setIndex(index)
            .setTerm(term)
            .setKind(RaftProtocol.EntryKind.ENTRY_CONFIGURATION)
            .setConfiguration(configuration(nodes))
            .build();
    }

    public static RaftProtocol.Configuration configuration(Collection<Node> nodes) {
        var builder = RaftProtocol.Configuration.newBuilder();

        for (var node : nodes) {
            builder.addPeers(RaftProtocol.Peer.newBuilder()
                .setId(node.id())
                .setHost(node.host())
                .setPort(node.port()));
        }

        return builder.build();
    }
}
