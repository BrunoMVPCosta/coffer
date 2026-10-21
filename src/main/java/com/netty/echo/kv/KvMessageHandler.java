package com.netty.echo.kv;

import java.util.concurrent.CompletableFuture;

import com.netty.echo.protocol.KvProtocol;

public interface KvMessageHandler {

    CompletableFuture<KvProtocol.KvResponse> handle(KvProtocol.KvCommand command);
}
