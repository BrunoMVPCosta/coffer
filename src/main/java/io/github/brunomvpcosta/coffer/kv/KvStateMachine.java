package io.github.brunomvpcosta.coffer.kv;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.brunomvpcosta.coffer.protocol.KvProtocol;
import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;
import io.github.brunomvpcosta.coffer.raft.RaftStateMachine;

/**
 * In-memory key-value state machine.
 *
 * <p>Only mutating commands are replicated; {@code GET} is served from the
 * local copy and is therefore eventually consistent on followers. Commands are
 * applied in Raft log order, so every replica ends up with the same map.</p>
 */
public class KvStateMachine implements RaftStateMachine {

    private final Map<String, byte[]> values = new ConcurrentHashMap<>();

    /**
     * Applies a committed command. Empty commands are Raft no-ops and are
     * ignored.
     */
    public void apply(KvProtocol.KvCommand command) {
        switch (command.getCommandCase()) {
            case PUT -> {
                var put = command.getPut();
                values.put(put.getKey(), put.getValue().toByteArray());
            }
            case DELETE -> values.remove(command.getDelete().getKey());
            case GET -> {
                // Reads are not replicated.
            }
            default -> {
                // No-op or unknown command.
            }
        }
    }

    @Override
    public void apply(RaftProtocol.LogEntry entry) {
        if (entry.getCommand().isEmpty()) {
            return;
        }

        try {
            apply(KvProtocol.KvCommand.parseFrom(entry.getCommand()));
        } catch (InvalidProtocolBufferException ex) {
            throw new IllegalStateException(
                "Invalid KV command at log index " + entry.getIndex(), ex);
        }
    }

    @Override
    public byte[] snapshot() {
        var builder = KvProtocol.KvSnapshot.newBuilder();

        values.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> builder.addEntries(KvProtocol.KvEntry.newBuilder()
                .setKey(entry.getKey())
                .setValue(ByteString.copyFrom(entry.getValue()))));

        return builder.build().toByteArray();
    }

    @Override
    public void restore(byte[] snapshot) {
        try {
            var parsed = KvProtocol.KvSnapshot.parseFrom(snapshot);
            values.clear();

            for (var entry : parsed.getEntriesList()) {
                values.put(entry.getKey(), entry.getValue().toByteArray());
            }
        } catch (InvalidProtocolBufferException ex) {
            throw new IllegalStateException("Invalid KV snapshot", ex);
        }
    }

    public Optional<byte[]> get(String key) {
        var value = values.get(key);
        return value == null ? Optional.empty() : Optional.of(value.clone());
    }

    public int size() {
        return values.size();
    }

    /**
     * @return the keys on this replica, sorted; may lag behind the leader
     */
    public List<String> keys() {
        return values.keySet().stream().sorted().toList();
    }
}
