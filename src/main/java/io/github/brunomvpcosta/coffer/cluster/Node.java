package io.github.brunomvpcosta.coffer.cluster;

import java.util.UUID;

public record Node(String id, String host, int port) {

    public Node(String host, int port) {
        this(UUID.randomUUID().toString(), host, port);
    }
}
