package com.netty.echo.transport.netty;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * NettyTransport
 */
public class NettyTransport implements Transport {

    static final String MESSAGE_ID_SEPARATOR = " ";

    private final Bootstrap bootstrap;
    private final ServerBootstrap serverBootstrap;
    private final ConcurrentMap<Address, Channel> connections = new ConcurrentHashMap<Address, Channel>();
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

        Channel channel = connections.get(address);
        if(channel == null) {
            ChannelFuture cf = bootstrap.connect(address.getHost(), address.getPort());
            cf.addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(!future.isSuccess()) {
                        System.out.println("Connection failed " + future.cause());
                        return;
                    }

                    Channel channel = future.channel();
                    channel.closeFuture().addListener(new CloseChannelListener());
                    connections.putIfAbsent(address, channel);

                    channel.writeAndFlush(framed);
                }
            });

        } else {
            channel.writeAndFlush(framed);
        }
    }

    /**
     * The listener to control when the connection is closed
     */
    private class CloseChannelListener implements ChannelFutureListener {

        public void operationComplete(ChannelFuture future) throws Exception {
            System.out.println("[CLIENT] - Connection closed");

            Address address = null;
            for(Map.Entry<Address, Channel> entry :  connections.entrySet()) {
                if(entry.getValue() == future.channel()) {
                    address = entry.getKey();
                    Channel channel = entry.getValue();
                    channel.disconnect();
                    break;
                }
            }

            if(address != null) {
                connections.remove(address);
            }
        }
    }

    @Override
    public byte[] sendAndReceive(Address address, byte[] payload) {
        UUID messageId = UUID.randomUUID();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        futures.put(messageId, future);

        String text = new String(payload, StandardCharsets.UTF_8);
        send(address, messageId + MESSAGE_ID_SEPARATOR + text);

        try {
           return future.get(1L * 3000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return "1".getBytes();
        }
    }
}