package io.github.brunomvpcosta.coffer.raft;

import io.github.brunomvpcosta.coffer.protocol.RaftProtocol;

public interface RaftMessageHandler {

    RaftProtocol.RaftMessage handle(RaftProtocol.RaftMessage message);
}
