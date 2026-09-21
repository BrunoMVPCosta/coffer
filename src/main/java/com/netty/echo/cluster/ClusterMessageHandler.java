package com.netty.echo.cluster;

import com.netty.echo.protocol.ClusterProtocol;

public interface ClusterMessageHandler {

    ClusterProtocol.ClusterMessage handle(ClusterProtocol.ClusterMessage message);
}
