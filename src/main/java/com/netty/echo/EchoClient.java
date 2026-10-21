package com.netty.echo;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.netty.echo.cluster.ClusterService;
import com.netty.echo.cluster.DefaultCluster;
import com.netty.echo.cluster.Node;
import com.netty.echo.kv.KvService;
import com.netty.echo.kv.KvStateMachine;
import com.netty.echo.net.Address;
import com.netty.echo.raft.FileRaftStorage;
import com.netty.echo.raft.RaftService;
import com.netty.echo.transport.netty.EchoClientInitializer;
import com.netty.echo.transport.netty.EchoServerInitializer;
import com.netty.echo.transport.netty.NettyTransport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * EchoClient
 */
public class EchoClient {

    public void start() throws Exception {
        System.out.println("[CLIENT] - Node is running");
        Thread.currentThread().join();
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("Usage " + EchoClient.class.getSimpleName() + " <port> <host>:<port>...");
            return;
        }

        int localPort = Integer.parseInt(args[0]);
        var localHost = advertisedHost();
        var localNode = new Node(localHost + ":" + localPort, localHost, localPort);

        var clusterName = System.getenv().getOrDefault("CLUSTER_NAME", "netty-cluster");

        // Wall-clock based incarnations make a restarted node win over the
        // tombstone left behind by its previous process.
        var cluster = new DefaultCluster(clusterName, localNode, System.currentTimeMillis());

        var seeds = new ArrayList<Address>();
        for(int i = 1; i < args.length; i++) {
            seeds.add(Address.from(args[i]));
        }

        var group = new NioEventLoopGroup();
        var futures = new ConcurrentHashMap<UUID, CompletableFuture<byte[]>>();

        var bootstrap = new Bootstrap();
        bootstrap
            .group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new EchoClientInitializer(futures));

        var serverGroup = new NioEventLoopGroup();

        var serverBootstrap = new ServerBootstrap();
        serverBootstrap
            .group(serverGroup)
            .channel(NioServerSocketChannel.class)
            .localAddress(new InetSocketAddress(localPort));

        var transport = new NettyTransport(bootstrap, futures);
        var clusterService = new ClusterService(cluster, transport, seeds);

        // Static Raft configuration: the seed roster is the authoritative
        // cluster. Every node must be given the same peers.
        var raftPeers = new ArrayList<Node>();
        for (var seed : seeds) {
            var peerId = seed.host() + ":" + seed.port();
            if (!peerId.equals(localNode.id())) {
                raftPeers.add(new Node(peerId, seed.host(), seed.port()));
            }
        }

        var raftService = new RaftService(localNode, raftPeers, transport,
            new FileRaftStorage(raftDataDirectory(localNode)));
        var kvService = new KvService(raftService, transport, new KvStateMachine());

        serverBootstrap
            .childHandler(new EchoServerInitializer(clusterService, raftService, kvService))
            .bind(localPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            raftService.stop();
            clusterService.leave();
        }));

        System.out.println("Cluster information");
        System.out.println("Cluster name: " + cluster.getName());
        System.out.println("Member: " + localNode.id());
        System.out.println("Seeds: ");
        for (var seed : seeds) {
            System.out.println("\tSeed: " + seed.host() + ":" + seed.port());
        }
        System.out.println("Raft state: " + raftDataDirectory(localNode).toAbsolutePath());

        clusterService.start();
        raftService.start();

        var client = new EchoClient();
        client.start();
    }

    private static Path raftDataDirectory(Node localNode) {
        var root = Path.of(System.getenv().getOrDefault("RAFT_DATA_DIR", "data"));
        return root.resolve(localNode.id().replace(':', '_'));
    }

    private static String advertisedHost() throws Exception {        var host = System.getenv("NODE_HOST");

        if (host == null || host.isBlank()) {
            return InetAddress.getLocalHost().getHostAddress();
        }

        return host;
    }
}
