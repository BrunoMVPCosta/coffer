package com.netty.echo.transport.netty;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netty.echo.net.Address;
import com.netty.echo.transport.Transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

/**
 * NettyTransport
 */
public class NettyTransport implements Transport {

    static final String MESSAGE_ID_SEPARATOR = " ";

    private final Bootstrap bootstrap;
    private final ServerBootstrap serverBootstrap;
    private final ConcurrentMap<Address, Channel> connections = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, CompletableFuture<byte[]>> futures;

    public NettyTransport(Bootstrap bootstrap, ServerBootstrap serverBootstrap, ConcurrentMap<UUID, CompletableFuture<byte[]>> futures) {
        this.bootstrap = bootstrap;
        this.serverBootstrap = serverBootstrap;
        this.futures = futures;
    }

    public void send(Address address, byte[] payload) {
        send(address, new String(payload, StandardCharsets.UTF_8));
    }

    /**
     * Send a message to a specific address.
     * @param address
     * @param message
     */
    private void send(Address address, String message) {
        final String framed = message.endsWith("\n") ? message : message + "\r\n";

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

                connectedChannel.writeAndFlush(framed);
            });

        } else {
            channel.writeAndFlush(framed);
        }
    }

    @Override
    public byte[] sendAndReceive(Address address, byte[] payload) {
        var messageId = UUID.randomUUID();
        var future = new CompletableFuture<byte[]>();
        futures.put(messageId, future);

        var text = new String(payload, StandardCharsets.UTF_8);
        send(address, messageId + MESSAGE_ID_SEPARATOR + text);

        try {
           return future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return "1".getBytes();
        }
    }
}
