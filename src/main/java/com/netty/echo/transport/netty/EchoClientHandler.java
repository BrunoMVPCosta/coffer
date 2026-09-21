package com.netty.echo.transport.netty;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * EchoClientHandler
 */
public class EchoClientHandler extends SimpleChannelInboundHandler<String> {

    private final ConcurrentMap<UUID, CompletableFuture<byte[]>> futures;

    public EchoClientHandler(ConcurrentMap<UUID, CompletableFuture<byte[]>> futures) {
        this.futures = futures;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String text) throws Exception {
        UUID messageId = parseMessageId(text);
        CompletableFuture<byte[]> future = messageId == null ? null : futures.remove(messageId);

        if (future != null) {
            int payloadStart = text.indexOf(NettyTransport.MESSAGE_ID_SEPARATOR) + NettyTransport.MESSAGE_ID_SEPARATOR.length();
            future.complete(text.substring(payloadStart).getBytes(StandardCharsets.UTF_8));
            return;
        }

        System.out.println("[CLIENT] - Received: " + text);
    }

    private UUID parseMessageId(String text) {
        int separatorIndex = text.indexOf(NettyTransport.MESSAGE_ID_SEPARATOR);

        if (separatorIndex < 0) {
            return null;
        }

        try {
            return UUID.fromString(text.substring(0, separatorIndex));
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