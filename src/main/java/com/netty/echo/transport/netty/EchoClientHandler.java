package com.netty.echo.transport.netty;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import com.netty.echo.protocol.ClusterProtocol;
import com.netty.echo.protocol.RaftProtocol;
import com.netty.echo.protocol.WireProtocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * EchoClientHandler
 */
public class EchoClientHandler extends SimpleChannelInboundHandler<WireProtocol.Envelope> {

    private final ConcurrentMap<UUID, CompletableFuture<byte[]>> futures;

    public EchoClientHandler(ConcurrentMap<UUID, CompletableFuture<byte[]>> futures) {
        this.futures = futures;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, WireProtocol.Envelope envelope) throws Exception {
        UUID messageId = parseMessageId(envelope.getMessageId());
        CompletableFuture<byte[]> future = messageId == null ? null : futures.remove(messageId);

        if (future != null) {
            future.complete(envelope.getPayload().toByteArray());
            return;
        }

        if (envelope.getKind() == WireProtocol.Envelope.Kind.CLUSTER) {
            var clusterMessage = ClusterProtocol.ClusterMessage.parseFrom(envelope.getPayload());
            System.out.println("[CLIENT] - Cluster message: " + clusterMessage.getPayloadCase());
            return;
        }

        if (envelope.getKind() == WireProtocol.Envelope.Kind.RAFT) {
            var raftMessage = RaftProtocol.RaftMessage.parseFrom(envelope.getPayload());
            System.out.println("[CLIENT] - Raft message: " + raftMessage.getPayloadCase());
            return;
        }

        System.out.println("[CLIENT] - Received: " + envelope.getPayload().toStringUtf8());
    }

    private UUID parseMessageId(String messageId) {
        if (messageId.isEmpty()) {
            return null;
        }

        try {
            return UUID.fromString(messageId);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
