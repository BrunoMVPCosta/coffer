package com.netty.echo.cluster;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netty.echo.net.Address;
import com.netty.echo.protocol.ClusterProtocol;
import com.netty.echo.transport.MessageKind;
import com.netty.echo.transport.Transport;

public class ClusterService implements ClusterMessageHandler {

    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(2);
    private static final int MAX_MISSED_HEARTBEATS = 3;

    private final Cluster cluster;
    private final Transport transport;
    private final Collection<Address> seeds;
    private final AtomicBoolean joined = new AtomicBoolean();
    private final ConcurrentMap<String, Integer> missedHeartbeats = new ConcurrentHashMap<>();
    private final Set<String> pendingHeartbeats = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
        var thread = new Thread(task, "cluster-heartbeat");
        thread.setDaemon(true);
        return thread;
    });

    public ClusterService(Cluster cluster, Transport transport, Collection<Address> seeds) {
        this.cluster = cluster;
        this.transport = transport;
        this.seeds = new ArrayList<>(seeds);
    }

    public void start() {
        joinSeeds();

        scheduler.scheduleWithFixedDelay(this::heartbeatMembers,
            HEARTBEAT_INTERVAL.toMillis(), HEARTBEAT_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void leave() {
        var message = ClusterMessages.leave(cluster.getMember().id()).toByteArray();

        for (var member : cluster.getMembers()) {
            transport.send(new Address(member.host(), member.port()), message, MessageKind.CLUSTER);
        }

        scheduler.shutdownNow();
    }

    @Override
    public ClusterProtocol.ClusterMessage handle(ClusterProtocol.ClusterMessage message) {
        switch (message.getPayloadCase()) {
            case JOIN -> {
                joined.set(true);
                var node = ClusterMessages.fromProto(message.getJoin().getNode());
                cluster.addMember(node);
                System.out.println("[CLUSTER] - " + node.id() + " joined, members: " + memberIds());
                broadcastMembers();
                return ClusterMessages.welcome(cluster.getName(), allMembers());
            }
            case PING -> {
                return ClusterMessages.pong(cluster.getMember());
            }
            case MEMBERS -> {
                updateMembers(message.getMembers().getMembersList());
                return null;
            }
            case LEAVE -> {
                cluster.removeMember(message.getLeave().getNodeId());
                System.out.println("[CLUSTER] - " + message.getLeave().getNodeId() + " left, members: " + memberIds());
                return null;
            }
            default -> {
                return null;
            }
        }
    }

    private void joinSeeds() {
        var join = ClusterMessages.join(cluster.getName(), cluster.getMember());

        for (var seed : seeds) {
            try {
                var response = transport.sendAndReceive(seed, join.toByteArray(), MessageKind.CLUSTER).get();
                var message = ClusterProtocol.ClusterMessage.parseFrom(response);

                if (message.hasWelcome()) {
                    joined.set(true);
                    updateMembers(message.getWelcome().getMembersList());
                    System.out.println("[CLUSTER] - Joined via " + address(seed) + ", members: " + memberIds());
                    return;
                }
            } catch (Exception ex) {
                System.out.println("[CLUSTER] - Could not join via " + address(seed) + ": " + ex.getMessage());
            }
        }
    }

    private void heartbeatMembers() {
        if (!joined.get()) {
            joinSeeds();
        }

        for (var member : cluster.getMembers()) {
            heartbeat(member);
        }
    }

    private void heartbeat(Node member) {
        if (!pendingHeartbeats.add(member.id())) {
            return;
        }

        var ping = ClusterMessages.ping(cluster.getMember()).toByteArray();

        transport.sendAndReceive(new Address(member.host(), member.port()), ping, MessageKind.CLUSTER)
            .whenComplete((response, cause) -> {
                pendingHeartbeats.remove(member.id());

                if (cause == null) {
                    heartbeatAnswered(member, response);
                } else {
                    heartbeatMissed(member);
                }
            });
    }

    private void heartbeatAnswered(Node member, byte[] response) {
        try {
            var message = ClusterProtocol.ClusterMessage.parseFrom(response);

            if (message.hasPong()) {
                missedHeartbeats.remove(member.id());
            }
        } catch (Exception ex) {
            heartbeatMissed(member);
        }
    }

    private void heartbeatMissed(Node member) {
        var missed = missedHeartbeats.merge(member.id(), 1, Integer::sum);

        if (missed < MAX_MISSED_HEARTBEATS) {
            return;
        }

        missedHeartbeats.remove(member.id());
        cluster.removeMember(member.id());
        System.out.println("[CLUSTER] - " + member.id() + " is unreachable, members: " + memberIds());
        broadcastMembers();
    }

    private void broadcastMembers() {
        var message = ClusterMessages.members(cluster.getName(), allMembers()).toByteArray();

        for (var member : cluster.getMembers()) {
            transport.send(new Address(member.host(), member.port()), message, MessageKind.CLUSTER);
        }
    }

    private void updateMembers(List<ClusterProtocol.Node> members) {
        cluster.setMembers(members.stream().map(ClusterMessages::fromProto).toList());
    }

    private List<Node> allMembers() {
        var members = new ArrayList<Node>();
        members.add(cluster.getMember());
        members.addAll(cluster.getMembers());
        return members;
    }

    private List<String> memberIds() {
        return allMembers().stream().map(Node::id).toList();
    }

    private String address(Address address) {
        return address.host() + ":" + address.port();
    }
}
