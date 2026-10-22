package io.github.brunomvpcosta.coffer.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.MessageLite;
import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

/**
 * Stores Raft metadata, an append-only log and the latest snapshot.
 *
 * <p>The term and vote live in a small metadata file. Log entries are written
 * to a length-prefixed append-only file, so a normal append does not rewrite
 * the whole log. The log file is rewritten only when entries are truncated or
 * compacted away. Snapshots live in their own file.</p>
 */
public class FileRaftStorage implements RaftStorage {

    private static final String META_FILE = "raft-meta.bin";
    private static final String META_TEMP = "raft-meta.bin.tmp";
    private static final String LOG_FILE = "raft-log.bin";
    private static final String LOG_TEMP = "raft-log.bin.tmp";
    private static final String SNAPSHOT_FILE = "raft-snapshot.bin";
    private static final String SNAPSHOT_TEMP = "raft-snapshot.bin.tmp";
    private static final String LEGACY_STATE_FILE = "raft-state.bin";

    private final Path directory;
    private final Path metaFile;
    private final Path logFile;
    private final Path snapshotFile;
    private final List<RaftProtocol.LogEntry> persistedLog = new ArrayList<>();
    private long persistedTerm;
    private String persistedVotedFor = "";

    public FileRaftStorage(Path directory) {
        this.directory = directory;
        this.metaFile = directory.resolve(META_FILE);
        this.logFile = directory.resolve(LOG_FILE);
        this.snapshotFile = directory.resolve(SNAPSHOT_FILE);

        try {
            Files.createDirectories(directory);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not create " + directory, ex);
        }
    }

    @Override
    public synchronized RaftProtocol.PersistentState load() {
        var meta = loadMeta();
        var log = loadLog();

        return RaftProtocol.PersistentState.newBuilder()
            .setCurrentTerm(meta.getCurrentTerm())
            .setVotedFor(meta.getVotedFor())
            .addAllLog(log)
            .build();
    }

    @Override
    public synchronized void save(RaftProtocol.PersistentState state) {
        saveMeta(state.getCurrentTerm(), state.getVotedFor());
        saveLog(state.getLogList());
    }

    @Override
    public synchronized RaftProtocol.Snapshot loadSnapshot() {
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
    public synchronized void saveSnapshot(RaftProtocol.Snapshot snapshot) {
        writeAtomically(snapshotFile, directory.resolve(SNAPSHOT_TEMP), snapshot);
    }

    private RaftProtocol.PersistentState loadMeta() {
        if (Files.exists(metaFile)) {
            try (var in = Files.newInputStream(metaFile)) {
                return RaftProtocol.PersistentState.parseFrom(in);
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not read " + metaFile, ex);
            }
        }

        // Migrate a state file written by the previous whole-log format.
        var legacy = directory.resolve(LEGACY_STATE_FILE);

        if (Files.exists(legacy)) {
            try (var in = Files.newInputStream(legacy)) {
                var state = RaftProtocol.PersistentState.parseFrom(in);
                var votedFor = state.getVotedFor();

                writeAtomically(metaFile, directory.resolve(META_TEMP),
                    RaftProtocol.PersistentState.newBuilder()
                        .setCurrentTerm(state.getCurrentTerm())
                        .setVotedFor(votedFor)
                        .build());

                persistedTerm = state.getCurrentTerm();
                persistedVotedFor = votedFor;
                saveLog(state.getLogList());
                Files.deleteIfExists(legacy);
                return state;
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not migrate " + legacy, ex);
            }
        }

        return RaftProtocol.PersistentState.getDefaultInstance();
    }

    private List<RaftProtocol.LogEntry> loadLog() {
        persistedLog.clear();

        if (!Files.exists(logFile)) {
            return persistedLog;
        }

        try {
            var buffer = ByteBuffer.wrap(Files.readAllBytes(logFile));

            while (buffer.hasRemaining()) {
                if (buffer.remaining() < Integer.BYTES) {
                    throw new IllegalStateException("Truncated log record in " + logFile);
                }

                var length = buffer.getInt();
                var bytes = new byte[length];
                buffer.get(bytes);
                persistedLog.add(RaftProtocol.LogEntry.parseFrom(bytes));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not read " + logFile, ex);
        }

        return persistedLog;
    }

    private void saveMeta(long term, String votedFor) {
        var normalised = votedFor == null ? "" : votedFor;

        if (term == persistedTerm && normalised.equals(persistedVotedFor)) {
            return;
        }

        writeAtomically(metaFile, directory.resolve(META_TEMP),
            RaftProtocol.PersistentState.newBuilder()
                .setCurrentTerm(term)
                .setVotedFor(normalised)
                .build());

        persistedTerm = term;
        persistedVotedFor = normalised;
    }

    private void saveLog(List<RaftProtocol.LogEntry> entries) {
        if (entries.size() >= persistedLog.size() && startsWith(entries, persistedLog)) {
            var appended = entries.subList(persistedLog.size(), entries.size());

            if (!appended.isEmpty()) {
                appendToLog(appended);
            }
        } else {
            rewriteLog(entries);
        }

        persistedLog.clear();
        persistedLog.addAll(entries);
    }

    private boolean startsWith(List<RaftProtocol.LogEntry> entries, List<RaftProtocol.LogEntry> prefix) {
        if (entries.size() < prefix.size()) {
            return false;
        }

        for (var index = 0; index < prefix.size(); index++) {
            if (!entries.get(index).equals(prefix.get(index))) {
                return false;
            }
        }

        return true;
    }

    private void appendToLog(List<RaftProtocol.LogEntry> entries) {
        var buffers = entries.stream().map(FileRaftStorage::encode).toArray(ByteBuffer[]::new);

        try (var channel = FileChannel.open(logFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND)) {

            while (channel.write(buffers) > 0) {
                // keep writing until every buffer is drained
            }

            channel.force(false);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not append to " + logFile, ex);
        }
    }

    private void rewriteLog(List<RaftProtocol.LogEntry> entries) {
        var temp = directory.resolve(LOG_TEMP);
        var buffers = entries.stream().map(FileRaftStorage::encode).toArray(ByteBuffer[]::new);

        try {
            try (var channel = FileChannel.open(temp,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

                while (channel.write(buffers) > 0) {
                    // keep writing until every buffer is drained
                }

                channel.force(true);
            }

            moveAtomically(temp, logFile);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not rewrite " + logFile, ex);
        }
    }

    private static ByteBuffer encode(RaftProtocol.LogEntry entry) {
        var bytes = entry.toByteArray();
        var buffer = ByteBuffer.allocate(Integer.BYTES + bytes.length);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
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

            moveAtomically(temp, target);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not write " + target, ex);
        }
    }

    private static void moveAtomically(Path temp, Path target) throws IOException {
        try {
            Files.move(temp, target,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
