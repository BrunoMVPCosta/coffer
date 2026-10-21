package com.netty.echo;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.netty.echo.net.Address;
import com.netty.echo.protocol.KvProtocol;
import com.netty.echo.transport.MessageKind;
import com.netty.echo.transport.netty.EchoClientInitializer;
import com.netty.echo.transport.netty.NettyTransport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Command-line client for the key-value store.
 *
 * <p>Writes may be sent to any node; followers forward them to the leader.
 * Reads are served by the contacted node and can be stale on a follower.</p>
 */
public final class KvClient {

    private KvClient() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }

        var command = command(args);

        if (command == null) {
            usage();
            return;
        }

        var address = Address.from(args[0]);
        var group = new NioEventLoopGroup();
        var futures = new ConcurrentHashMap<UUID, CompletableFuture<byte[]>>();

        var bootstrap = new Bootstrap()
            .group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new EchoClientInitializer(futures));

        try {
            var transport = new NettyTransport(bootstrap, futures);
            var payload = transport.sendAndReceive(address, command.toByteArray(), MessageKind.KV)
                .get(5, TimeUnit.SECONDS);

            var response = KvProtocol.KvResponse.parseFrom(payload);
            print(response);

            if (response.getStatus() == KvProtocol.KvResponse.Status.STATUS_ERROR
                || response.getStatus() == KvProtocol.KvResponse.Status.STATUS_NOT_LEADER) {
                System.exit(1);
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    private static KvProtocol.KvCommand command(String[] args) {
        return switch (args[1]) {
            case "put" -> {
                if (args.length != 4) {
                    yield null;
                }

                yield KvProtocol.KvCommand.newBuilder()
                    .setPut(KvProtocol.Put.newBuilder()
                        .setKey(args[2])
                        .setValue(ByteString.copyFromUtf8(args[3])))
                    .build();
            }
            case "get" -> {
                if (args.length != 3) {
                    yield null;
                }

                yield KvProtocol.KvCommand.newBuilder()
                    .setGet(KvProtocol.Get.newBuilder().setKey(args[2]))
                    .build();
            }
            case "delete" -> {
                if (args.length != 3) {
                    yield null;
                }

                yield KvProtocol.KvCommand.newBuilder()
                    .setDelete(KvProtocol.Delete.newBuilder().setKey(args[2]))
                    .build();
            }
            default -> null;
        };
    }

    private static void print(KvProtocol.KvResponse response) {
        switch (response.getStatus()) {
            case STATUS_OK -> {
                if (response.getHasValue()) {
                    System.out.println(response.getValue().toStringUtf8());
                } else {
                    System.out.println("OK");
                }
            }
            case STATUS_NOT_FOUND -> System.out.println("(not found)");
            case STATUS_NOT_LEADER -> System.out.println("not leader"
                + (response.getLeaderId().isEmpty() ? "" : "; try " + response.getLeaderId()));
            case STATUS_ERROR -> System.out.println("error: " + response.getError());
            default -> System.out.println(response.getStatus());
        }
    }

    private static void usage() {
        System.err.println("Usage: KvClient <host:port> put <key> <value>");
        System.err.println("       KvClient <host:port> get <key>");
        System.err.println("       KvClient <host:port> delete <key>");
    }
}
