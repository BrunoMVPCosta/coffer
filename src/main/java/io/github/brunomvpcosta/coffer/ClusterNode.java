package io.github.brunomvpcosta.coffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.github.brunomvpcosta.coffer.cluster.ClusterService;
import io.github.brunomvpcosta.coffer.cluster.DefaultCluster;
import io.github.brunomvpcosta.coffer.cluster.Node;
import io.github.brunomvpcosta.coffer.kv.KvService;
import io.github.brunomvpcosta.coffer.kv.KvStateMachine;
import io.github.brunomvpcosta.coffer.net.Address;
import io.github.brunomvpcosta.coffer.raft.FileRaftStorage;
import io.github.brunomvpcosta.coffer.raft.RaftService;
import io.github.brunomvpcosta.coffer.transport.netty.RpcClientInitializer;
import io.github.brunomvpcosta.coffer.transport.netty.RpcServerInitializer;
import io.github.brunomvpcosta.coffer.transport.netty.NettyTransport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Coffer cluster node.
 */
public class ClusterNode {

    public void start() throws Exception {
        System.out.println("[CLIENT] - Node is running");
        Thread.currentThread().join();
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("Usage " + ClusterNode.class.getSimpleName() + " <port> <host>:<port>...");
            return;
        }

        int localPort = Integer.parseInt(args[0]);
        var localHost = advertisedHost();
        var localNode = new Node(localHost + ":" + localPort, localHost, localPort);

        var clusterName = System.getenv().getOrDefault("CLUSTER_NAME", "coffer-cluster");

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
            .handler(new RpcClientInitializer(futures));

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

        var snapshotThreshold = Long.parseLong(
            System.getenv().getOrDefault("RAFT_SNAPSHOT_THRESHOLD", "1024"));
        var raftService = new RaftService(localNode, raftPeers, transport,
            new FileRaftStorage(raftDataDirectory(localNode)), snapshotThreshold);
        var kvService = new KvService(raftService, transport, new KvStateMachine());

        serverBootstrap
            .childHandler(new RpcServerInitializer(clusterService, raftService, kvService))
            .bind(localPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            raftService.stop();
            clusterService.leave();
        }));

        System.out.println("Coffer - Raft-backed key-value cluster");
        System.out.println();
        System.out.println("Cluster information");
        System.out.println("Cluster name: " + cluster.getName());
        System.out.println("Member: " + localNode.id());
        System.out.println("Seeds: ");
        for (var seed : seeds) {
            System.out.println("\tSeed: " + seed.host() + ":" + seed.port());
        }
        System.out.println("Raft state: " + raftDataDirectory(localNode).toAbsolutePath());
        System.out.println("Snapshot threshold: " + snapshotThreshold + " applied entries");

        clusterService.start();
        raftService.start();

        var client = new ClusterNode();
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
