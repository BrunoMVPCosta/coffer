package io.github.brunomvpcosta.coffer.transport;

import java.util.concurrent.CompletableFuture;

import io.github.brunomvpcosta.coffer.net.Address;

/**
 * Transport
 */
public interface Transport {
    /**
     * Send a message to a specific address
     */
    default void send(Address address, byte[] payload) {
        send(address, payload, MessageKind.APPLICATION);
    }

    void send(Address address, byte[] payload, MessageKind kind);

    /**
     * Send and waits for the reply
     * @param address
     * @param payload
     * @return
     */
    default CompletableFuture<byte[]> sendAndReceive(Address address, byte[] payload) {
        return sendAndReceive(address, payload, MessageKind.APPLICATION);
    }

    CompletableFuture<byte[]> sendAndReceive(Address address, byte[] payload, MessageKind kind);
}
