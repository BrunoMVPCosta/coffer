package io.github.brunomvpcosta.coffer.cluster;

import io.github.brunomvpcosta.coffer.protocol.ClusterProtocol;

public interface ClusterMessageHandler {

    ClusterProtocol.ClusterMessage handle(ClusterProtocol.ClusterMessage message);
}
