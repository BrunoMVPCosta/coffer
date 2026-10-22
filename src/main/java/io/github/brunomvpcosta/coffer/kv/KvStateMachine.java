package io.github.brunomvpcosta.coffer.kv;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.github.brunomvpcosta.coffer.protocol.KvProtocol;

/**
 * In-memory key-value state machine.
 *
 * <p>Only mutating commands are replicated; {@code GET} is served from the
 * local copy and is therefore eventually consistent on followers. Commands are
 * applied in Raft log order, so every replica ends up with the same map.</p>
 */
public class KvStateMachine {

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
