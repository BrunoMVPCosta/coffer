package com.netty.echo.raft;

import com.netty.echo.protocol.RaftProtocol;

public interface RaftMessageHandler {

    RaftProtocol.RaftMessage handle(RaftProtocol.RaftMessage message);
}
