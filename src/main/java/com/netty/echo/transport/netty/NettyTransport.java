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
 *
 * <p>Keeps one pooled connection per address. A connection is reused while it
 * is active; when it closes or a write fails, the next call opens a new one.
 * Concurrent callers racing to the same address share a single in-flight
 * connect attempt.</p>
 */
public class NettyTransport implements Transport {

    private static final long REQUEST_TIMEOUT_SECONDS = 3;

    private final Bootstrap bootstrap;
    private final ServerBootstrap serverBootstrap;
    private final ConcurrentMap<Address, Channel> connections = new ConcurrentHashMap<>();
    private final ConcurrentMap<Address, CompletableFuture<Channel>> pendingConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, CompletableFuture<byte[]>> futures;

    public NettyTransport(Bootstrap bootstrap, ServerBootstrap serverBootstrap, ConcurrentMap<UUID, CompletableFuture<byte[]>> futures) {
        this.bootstrap = bootstrap;
        this.serverBootstrap = serverBootstrap;
        this.futures = futures;
    }

    @Override
    public void send(Address address, byte[] payload, MessageKind kind) {
        var envelope = WireProtocol.Envelope.newBuilder()
            .setKind(toProtoKind(kind))
            .setPayload(ByteString.copyFrom(payload))
            .build();

        channel(address).whenComplete((channel, cause) -> {
            if (cause != null) {
                log(address, "connect failed", cause);
                return;
            }

            channel.writeAndFlush(envelope).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    log(address, "send failed", future.cause());
                }
            });
        });
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(Address address, byte[] payload, MessageKind kind) {
        var messageId = UUID.randomUUID();
        var future = new CompletableFuture<byte[]>();
        futures.put(messageId, future);

        var envelope = WireProtocol.Envelope.newBuilder()
            .setMessageId(messageId.toString())
            .setKind(toProtoKind(kind))
            .setPayload(ByteString.copyFrom(payload))
            .build();

        channel(address).whenComplete((channel, cause) -> {
            if (cause != null) {
                futures.remove(messageId);
                future.completeExceptionally(cause);
                return;
            }

            channel.writeAndFlush(envelope).addListener((ChannelFutureListener) write -> {
                if (!write.isSuccess()) {
                    futures.remove(messageId);
                    future.completeExceptionally(write.cause());
                }
            });
        });

        return future
            .orTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .whenComplete((response, cause) -> futures.remove(messageId));
    }

    /**
     * Returns an active channel for the address, opening one if needed. All
     * callers arriving during a connect share the same future.
     */
    private CompletableFuture<Channel> channel(Address address) {
        var existing = connections.get(address);

        if (existing != null && existing.isActive()) {
            return CompletableFuture.completedFuture(existing);
        }

        if (existing != null) {
            connections.remove(address, existing);
        }

        var pending = pendingConnections.get(address);
        if (pending != null) {
            return pending;
        }

        var created = new CompletableFuture<Channel>();
        pending = pendingConnections.putIfAbsent(address, created);

        if (pending != null) {
            return pending;
        }

        connect(address, created);
        return created;
    }

    private void connect(Address address, CompletableFuture<Channel> promise) {
        bootstrap.connect(address.host(), address.port()).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                pendingConnections.remove(address, promise);
                promise.completeExceptionally(future.cause());
                return;
            }

            var channel = future.channel();

            channel.closeFuture().addListener((ChannelFutureListener) close ->
                connections.remove(address, channel));

            connections.put(address, channel);
            pendingConnections.remove(address, promise);
            promise.complete(channel);
        });
    }

    private void log(Address address, String action, Throwable cause) {
        System.out.println("[CLIENT] - " + action + " for " + address.host() + ":" + address.port() + ": " + cause);
    }

    private WireProtocol.Envelope.Kind toProtoKind(MessageKind kind) {
        return switch (kind) {
            case CLUSTER -> WireProtocol.Envelope.Kind.CLUSTER;
            case RAFT -> WireProtocol.Envelope.Kind.RAFT;
            case APPLICATION -> WireProtocol.Envelope.Kind.APPLICATION;
        };
    }
}
