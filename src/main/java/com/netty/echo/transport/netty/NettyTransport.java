package com.netty.echo.transport.netty;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.netty.echo.net.Address;
import com.netty.echo.protocol.WireProtocol;
import com.netty.echo.transport.MessageKind;
import com.netty.echo.transport.Transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

/**
 * NettyTransport
 */
public class NettyTransport implements Transport {

    private final Bootstrap bootstrap;
    private final ServerBootstrap serverBootstrap;
    private final ConcurrentMap<Address, Channel> connections = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, CompletableFuture<byte[]>> futures;

    public NettyTransport(Bootstrap bootstrap, ServerBootstrap serverBootstrap, ConcurrentMap<UUID, CompletableFuture<byte[]>> futures) {
        this.bootstrap = bootstrap;
        this.serverBootstrap = serverBootstrap;
        this.futures = futures;
    }

    @Override
    public void send(Address address, byte[] payload, MessageKind kind) {
        send(address, WireProtocol.Envelope.newBuilder()
            .setKind(toProtoKind(kind))
            .setPayload(ByteString.copyFrom(payload))
            .build());
    }

    private void send(Address address, WireProtocol.Envelope envelope) {
        var channel = connections.get(address);
        if(channel == null) {
            bootstrap.connect(address.host(), address.port()).addListener((ChannelFutureListener) future -> {
                if(!future.isSuccess()) {
                    System.out.println("Connection failed " + future.cause());
                    return;
                }

                var connectedChannel = future.channel();
                connectedChannel.closeFuture().addListener((ChannelFutureListener) closeFuture -> {
                    System.out.println("[CLIENT] - Connection closed");
                    connections.values().remove(connectedChannel);
                });
                connections.putIfAbsent(address, connectedChannel);

                connectedChannel.writeAndFlush(envelope);
            });

        } else {
            channel.writeAndFlush(envelope);
        }
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(Address address, byte[] payload, MessageKind kind) {
        var messageId = UUID.randomUUID();
        var future = new CompletableFuture<byte[]>();
        futures.put(messageId, future);

        send(address, WireProtocol.Envelope.newBuilder()
            .setMessageId(messageId.toString())
            .setKind(toProtoKind(kind))
            .setPayload(ByteString.copyFrom(payload))
            .build());

        return future
            .orTimeout(3, TimeUnit.SECONDS)
            .whenComplete((response, cause) -> futures.remove(messageId));
    }

    private WireProtocol.Envelope.Kind toProtoKind(MessageKind kind) {
        return kind == MessageKind.CLUSTER
            ? WireProtocol.Envelope.Kind.CLUSTER
            : WireProtocol.Envelope.Kind.APPLICATION;
    }
}
