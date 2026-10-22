package io.github.brunomvpcosta.coffer.raft;

import java.util.Collection;

import com.google.protobuf.ByteString;
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

    public static RaftProtocol.LogEntry entry(long index, long term, byte[] command) {
        return RaftProtocol.LogEntry.newBuilder()
            .setIndex(index)
            .setTerm(term)
            .setCommand(ByteString.copyFrom(command))
            .build();
    }
}
