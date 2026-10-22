package io.github.brunomvpcosta.coffer.kv;

import java.util.concurrent.CompletableFuture;

import io.github.brunomvpcosta.coffer.protocol.KvProtocol;

public interface KvMessageHandler {

    CompletableFuture<KvProtocol.KvResponse> handle(KvProtocol.KvCommand command);
}
