package io.github.brunomvpcosta.coffer.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import com.google.protobuf.MessageLite;
import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

/**
 * Stores the Raft term, vote and compacted log in one file, and the latest
 * snapshot in another.
 *
 * <p>Each save writes a temporary file, forces it to disk, and atomically
 * replaces the previous state. The log is rewritten on every change, so
 * snapshots are what keep it bounded.</p>
 */
public class FileRaftStorage implements RaftStorage {

    private static final String STATE_FILE = "raft-state.bin";
    private static final String STATE_TEMP = "raft-state.bin.tmp";
    private static final String SNAPSHOT_FILE = "raft-snapshot.bin";
    private static final String SNAPSHOT_TEMP = "raft-snapshot.bin.tmp";

    private final Path directory;
    private final Path stateFile;
    private final Path snapshotFile;

    public FileRaftStorage(Path directory) {
        this.directory = directory;
        this.stateFile = directory.resolve(STATE_FILE);
        this.snapshotFile = directory.resolve(SNAPSHOT_FILE);

        try {
            Files.createDirectories(directory);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not create " + directory, ex);
        }
    }

    @Override
    public RaftProtocol.PersistentState load() {
        if (!Files.exists(stateFile)) {
            return RaftProtocol.PersistentState.getDefaultInstance();
        }

        try (var in = Files.newInputStream(stateFile)) {
            return RaftProtocol.PersistentState.parseFrom(in);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not read " + stateFile, ex);
        }
    }

    @Override
    public void save(RaftProtocol.PersistentState state) {
        writeAtomically(stateFile, directory.resolve(STATE_TEMP), state);
    }

    @Override
    public RaftProtocol.Snapshot loadSnapshot() {
        if (!Files.exists(snapshotFile)) {
            return RaftProtocol.Snapshot.getDefaultInstance();
        }

        try (var in = Files.newInputStream(snapshotFile)) {
            return RaftProtocol.Snapshot.parseFrom(in);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not read " + snapshotFile, ex);
        }
    }

    @Override
    public void saveSnapshot(RaftProtocol.Snapshot snapshot) {
        writeAtomically(snapshotFile, directory.resolve(SNAPSHOT_TEMP), snapshot);
    }

    private void writeAtomically(Path target, Path temp, MessageLite message) {
        try {
            try (var channel = FileChannel.open(temp,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
                var out = Channels.newOutputStream(channel)) {

                message.writeTo(out);
                out.flush();
                channel.force(true);
            }

            try {
                Files.move(temp, target,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not write " + target, ex);
        }
    }
}
