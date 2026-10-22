package io.github.brunomvpcosta.coffer.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

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

    @Test
    void roundTripsSnapshot() {
        var storage = new FileRaftStorage(directory);
        var snapshot = RaftMessages.snapshot(5, 2, "kv-state".getBytes(StandardCharsets.UTF_8));

        storage.saveSnapshot(snapshot);

        assertEquals(snapshot, storage.loadSnapshot());
    }

    @Test
    void missingSnapshotReturnsEmptyState() {
        var storage = new FileRaftStorage(directory);

        assertEquals(0, storage.loadSnapshot().getLastIncludedIndex());
    }

    @Test
    void appendsOnlyNewEntries() {
        var storage = new FileRaftStorage(directory);
        var first = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(1)
            .addLog(RaftMessages.entry(1, 1, bytes("a")))
            .build();
        var second = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(1)
            .addLog(RaftMessages.entry(1, 1, bytes("a")))
            .addLog(RaftMessages.entry(2, 1, bytes("b")))
            .build();

        storage.save(first);
        storage.save(second);

        assertEquals(second, storage.load());
    }

    @Test
    void rewritesLogWhenEntriesAreTruncated() {
        var storage = new FileRaftStorage(directory);
        var three = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(1)
            .addLog(RaftMessages.entry(1, 1, bytes("a")))
            .addLog(RaftMessages.entry(2, 1, bytes("b")))
            .addLog(RaftMessages.entry(3, 1, bytes("c")))
            .build();
        var two = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(1)
            .addLog(RaftMessages.entry(1, 1, bytes("a")))
            .addLog(RaftMessages.entry(2, 1, bytes("b")))
            .build();

        storage.save(three);
        storage.save(two);

        assertEquals(two, storage.load());
    }

    @Test
    void rewritesLogWhenEntriesAreCompacted() {
        var storage = new FileRaftStorage(directory);
        var three = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(1)
            .addLog(RaftMessages.entry(1, 1, bytes("a")))
            .addLog(RaftMessages.entry(2, 1, bytes("b")))
            .addLog(RaftMessages.entry(3, 1, bytes("c")))
            .build();
        var compacted = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(1)
            .addLog(RaftMessages.entry(3, 1, bytes("c")))
            .build();

        storage.save(three);
        storage.save(compacted);

        assertEquals(compacted, storage.load());
    }

    @Test
    void migratesTheLegacyStateFile() throws Exception {
        var legacy = RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(4)
            .setVotedFor("node-2")
            .addLog(RaftMessages.entry(1, 4, bytes("a")))
            .build();
        Files.write(directory.resolve("raft-state.bin"), legacy.toByteArray());

        var storage = new FileRaftStorage(directory);

        assertEquals(legacy, storage.load());
        assertFalse(Files.exists(directory.resolve("raft-state.bin")));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
