package com.netty.echo.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import com.netty.echo.protocol.RaftProtocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRaftStorageTest {

    @TempDir
    Path directory;

    @Test
    void missingFileReturnsEmptyState() {
        var storage = new FileRaftStorage(directory);
        var state = storage.load();

        assertEquals(0, state.getCurrentTerm());
        assertTrue(state.getLogList().isEmpty());
    }

    @Test
    void roundTripsState() {
        var storage = new FileRaftStorage(directory);
        var state = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(7)
            .setVotedFor("node-2")
            .addLog(RaftMessages.entry(1, 7, "a".getBytes(StandardCharsets.UTF_8)))
            .build();

        storage.save(state);

        assertEquals(state, storage.load());
    }

    @Test
    void overwritesPreviousState() {
        var storage = new FileRaftStorage(directory);

        storage.save(RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(1)
            .setVotedFor("node-2")
            .build());
        storage.save(RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(2)
            .setVotedFor("node-3")
            .build());

        var state = storage.load();

        assertEquals(2, state.getCurrentTerm());
        assertEquals("node-3", state.getVotedFor());
    }
}
