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

import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

/**
 * Stores the Raft term, vote and log in a single protobuf file.
 *
 * <p>Each save writes a temporary file, forces it to disk, and atomically
 * replaces the previous state. This is intentionally simple: the whole log is
 * rewritten on every change, which is fine for a small cluster but demands
 * snapshots and a segmented log later.</p>
 */
public class FileRaftStorage implements RaftStorage {

    private static final String STATE_FILE = "raft-state.bin";
    private static final String TEMP_FILE = "raft-state.bin.tmp";

    private final Path directory;
    private final Path stateFile;

    public FileRaftStorage(Path directory) {
        this.directory = directory;
        this.stateFile = directory.resolve(STATE_FILE);

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
        var temp = directory.resolve(TEMP_FILE);

        try {
            try (var channel = FileChannel.open(temp,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
                var out = Channels.newOutputStream(channel)) {

                state.writeTo(out);
                out.flush();
                channel.force(true);
            }

            try {
                Files.move(temp, stateFile,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(temp, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not write " + stateFile, ex);
        }
    }
}
