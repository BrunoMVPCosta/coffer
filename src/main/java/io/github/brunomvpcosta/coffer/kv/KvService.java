package io.github.brunomvpcosta.coffer.kv;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.brunomvpcosta.coffer.net.Address;
import io.github.brunomvpcosta.coffer.protocol.KvProtocol;
import io.github.brunomvpcosta.coffer.raft.RaftService;
import io.github.brunomvpcosta.coffer.transport.MessageKind;
import io.github.brunomvpcosta.coffer.transport.Transport;

/**
 * Serves key-value commands on top of the Raft log.
 *
 * <p>Writes are accepted only by the leader; a follower forwards them to the
 * leader it currently knows about. Reads are served from the local replica, so
 * a follower read can be stale. Use the leader for read-after-write
 * consistency.</p>
 */
public class KvService implements KvMessageHandler {

    private static final long WRITE_TIMEOUT_SECONDS = 5;

    private final RaftService raft;
    private final Transport transport;
    private final KvStateMachine state;

    public KvService(RaftService raft, Transport transport, KvStateMachine state) {
        this.raft = raft;
        this.transport = transport;
        this.state = state;

        raft.setApplier(entry -> {
            if (entry.getCommand().isEmpty()) {
                return;
            }

            try {
                state.apply(KvProtocol.KvCommand.parseFrom(entry.getCommand()));
            } catch (InvalidProtocolBufferException ex) {
                throw new IllegalStateException(
                    "Invalid KV command at log index " + entry.getIndex(), ex);
            }
        });
    }

    @Override
    public CompletableFuture<KvProtocol.KvResponse> handle(KvProtocol.KvCommand command) {
        return switch (command.getCommandCase()) {
            case GET -> CompletableFuture.completedFuture(read(command.getGet()));
            case INFO -> CompletableFuture.completedFuture(info());
            case PUT, DELETE -> write(command);
            default -> CompletableFuture.completedFuture(error("empty command"));
        };
    }

    private KvProtocol.KvResponse info() {
        return KvProtocol.KvResponse.newBuilder()
            .setStatus(KvProtocol.KvResponse.Status.STATUS_OK)
            .setRole(raft.role().name())
            .setTerm(raft.currentTerm())
            .setLeaderId(raft.leaderId() == null ? "" : raft.leaderId())
            .addAllKeys(state.keys())
            .build();
    }

    private KvProtocol.KvResponse read(KvProtocol.Get get) {
        if (get.getKey().isBlank()) {
            return error("key cannot be empty");
        }

        var value = state.get(get.getKey());

        if (value.isEmpty()) {
            return KvProtocol.KvResponse.newBuilder()
                .setStatus(KvProtocol.KvResponse.Status.STATUS_NOT_FOUND)
                .build();
        }

        return KvProtocol.KvResponse.newBuilder()
            .setStatus(KvProtocol.KvResponse.Status.STATUS_OK)
            .setValue(ByteString.copyFrom(value.get()))
            .setHasValue(true)
            .build();
    }

    private CompletableFuture<KvProtocol.KvResponse> write(KvProtocol.KvCommand command) {
        var key = switch (command.getCommandCase()) {
            case PUT -> command.getPut().getKey();
            case DELETE -> command.getDelete().getKey();
            default -> "";
        };

        if (key.isBlank()) {
            return CompletableFuture.completedFuture(error("key cannot be empty"));
        }

        if (!raft.isLeader()) {
            return forward(command);
        }

        return raft.append(command.toByteArray())
            .thenApply(index -> KvProtocol.KvResponse.newBuilder()
                .setStatus(KvProtocol.KvResponse.Status.STATUS_OK)
                .build())
            .orTimeout(WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .exceptionally(this::error);
    }

    private CompletableFuture<KvProtocol.KvResponse> forward(KvProtocol.KvCommand command) {
        var leader = raft.leader();

        if (leader == null || leader.id().equals(raft.self().id())) {
            return CompletableFuture.completedFuture(notLeader(null));
        }

        return transport.sendAndReceive(
                new Address(leader.host(), leader.port()),
                command.toByteArray(),
                MessageKind.KV)
            .thenApply(this::parseResponse)
            .exceptionally(cause -> notLeader(leader.id()));
    }

    private KvProtocol.KvResponse parseResponse(byte[] payload) {
        try {
            return KvProtocol.KvResponse.parseFrom(payload);
        } catch (InvalidProtocolBufferException ex) {
            return error("Invalid KV response: " + ex.getMessage());
        }
    }

    private KvProtocol.KvResponse notLeader(String leaderId) {
        var builder = KvProtocol.KvResponse.newBuilder()
            .setStatus(KvProtocol.KvResponse.Status.STATUS_NOT_LEADER);

        if (leaderId != null) {
            builder.setLeaderId(leaderId);
        }

        return builder.build();
    }

    private KvProtocol.KvResponse error(String message) {
        return KvProtocol.KvResponse.newBuilder()
            .setStatus(KvProtocol.KvResponse.Status.STATUS_ERROR)
            .setError(message)
            .build();
    }

    private KvProtocol.KvResponse error(Throwable cause) {
        return error(cause.getMessage() == null ? cause.toString() : cause.getMessage());
    }
}
