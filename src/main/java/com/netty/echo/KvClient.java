package com.netty.echo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netty.echo.kv.KvCommandParser;
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
 * Command-line and interactive client for the key-value store.
 *
 * <p>Writes may be sent to any node; followers forward them to the leader.
 * Reads are served by the contacted node and can be stale on a follower.</p>
 *
 * <p>With only an address, the client opens an interactive shell and keeps one
 * connection open for the whole session. With a command after the address, it
 * runs once and exits, like {@code redis-cli}.</p>
 */
public final class KvClient {

    private static final long REQUEST_TIMEOUT_SECONDS = 5;
    private static final String PROMPT = "> ";

    private KvClient() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }

        var address = Address.from(args[0]);

        if (args.length == 1) {
            interactive(address);
            return;
        }

        var name = args[1].toLowerCase();
        var command = KvCommandParser.parse(
            String.join(" ", Arrays.copyOfRange(args, 1, args.length)));

        if (command == null) {
            usage();
            return;
        }

        int exitCode;

        try (var connection = new Connection()) {
            exitCode = print(connection.send(address, command), name);
        }

        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static void interactive(Address address) throws Exception {
        var showPrompt = System.console() != null;
        var endpoint = endpoint(address);

        try (var connection = new Connection();
            var reader = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("Connected to " + endpoint);
            System.out.println("Type 'help' for commands, 'quit' to exit.");

            while (true) {
                if (showPrompt) {
                    System.out.print(endpoint + PROMPT);
                    System.out.flush();
                }

                var line = reader.readLine();

                if (line == null) {
                    break;
                }

                var trimmed = line.trim();

                if (trimmed.isEmpty()) {
                    continue;
                }

                var name = trimmed.split("\\s+", 2)[0].toLowerCase();

                switch (name) {
                    case "quit", "exit" -> {
                        return;
                    }
                    case "help" -> {
                        help();
                        continue;
                    }
                    default -> {
                    }
                }

                var command = KvCommandParser.parse(trimmed);

                if (command == null) {
                    System.out.println("(error) unknown command; type 'help'");
                    continue;
                }

                try {
                    print(connection.send(address, command), name);
                } catch (Exception ex) {
                    System.out.println("(error) " + message(ex));
                }
            }
        }
    }

    private static int print(KvProtocol.KvResponse response, String name) {
        switch (response.getStatus()) {
            case STATUS_OK -> {
                switch (name) {
                    case "keys" -> {
                        if (response.getKeysCount() == 0) {
                            System.out.println("(empty)");
                        } else {
                            response.getKeysList().forEach(System.out::println);
                        }
                    }
                    case "leader" -> System.out.println(
                        response.getLeaderId().isEmpty() ? "(unknown)" : response.getLeaderId());
                    case "info", "status" -> {
                        System.out.println("role: " + response.getRole());
                        System.out.println("term: " + response.getTerm());
                        System.out.println("leader: "
                            + (response.getLeaderId().isEmpty() ? "(unknown)" : response.getLeaderId()));
                        System.out.println("keys: " + response.getKeysCount());
                    }
                    default -> {
                        if (response.getHasValue()) {
                            System.out.println(response.getValue().toStringUtf8());
                        } else {
                            System.out.println("OK");
                        }
                    }
                }

                return 0;
            }
            case STATUS_NOT_FOUND -> {
                System.out.println("(not found)");
                return 0;
            }
            case STATUS_NOT_LEADER -> {
                System.out.println("(error) not leader"
                    + (response.getLeaderId().isEmpty() ? "" : "; try " + response.getLeaderId()));
                return 1;
            }
            case STATUS_ERROR -> {
                System.out.println("(error) " + response.getError());
                return 1;
            }
            default -> {
                System.out.println("(error) unexpected status " + response.getStatus());
                return 1;
            }
        }
    }

    private static void help() {
        System.out.println("put <key> <value>   store a value");
        System.out.println("get <key>           read a value");
        System.out.println("del <key>           delete a key");
        System.out.println("keys                list keys on the contacted node");
        System.out.println("leader              show the leader known to this node");
        System.out.println("info                role, term, leader and key count");
        System.out.println("help                show this help");
        System.out.println("quit                leave the shell");
    }

    private static void usage() {
        System.err.println("Usage: KvClient <host:port>");
        System.err.println("       KvClient <host:port> put <key> <value>");
        System.err.println("       KvClient <host:port> get <key>");
        System.err.println("       KvClient <host:port> del <key>");
        System.err.println("       KvClient <host:port> keys|leader|info");
    }

    private static String endpoint(Address address) {
        return address.host() + ":" + address.port();
    }

    private static String message(Throwable cause) {
        var root = cause.getCause() == null ? cause : cause.getCause();
        return root.getMessage() == null ? root.toString() : root.getMessage();
    }

    private static final class Connection implements AutoCloseable {

        private final NioEventLoopGroup group;
        private final NettyTransport transport;

        private Connection() {
            this.group = new NioEventLoopGroup();

            var futures = new ConcurrentHashMap<UUID, CompletableFuture<byte[]>>();
            var bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new EchoClientInitializer(futures));

            this.transport = new NettyTransport(bootstrap, futures);
        }

        private KvProtocol.KvResponse send(Address address, KvProtocol.KvCommand command) throws Exception {
            var payload = transport.sendAndReceive(address, command.toByteArray(), MessageKind.KV)
                .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return KvProtocol.KvResponse.parseFrom(payload);
        }

        @Override
        public void close() {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }
}
