package io.github.brunomvpcosta.coffer.cluster;

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

import io.github.brunomvpcosta.coffer.net.Address;
import io.github.brunomvpcosta.coffer.protocol.ClusterProtocol;
import io.github.brunomvpcosta.coffer.transport.MessageKind;
import io.github.brunomvpcosta.coffer.transport.Transport;

public class ClusterService implements ClusterMessageHandler {

    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(2);
    private static final int MAX_MISSED_HEARTBEATS = 3;
    private static final Duration TOMBSTONE_TTL = Duration.ofMinutes(5);

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
        var local = cluster.getLocalMember();
        var message = ClusterMessages.leave(local).toByteArray();

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
                var member = ClusterMessages.memberFromProto(message.getJoin().getNode());
                cluster.merge(List.of(member));
                System.out.println("[CLUSTER] - " + member.node().id() + " joined, members: " + memberIds());
                broadcastMembers();
                return ClusterMessages.welcome(cluster.getName(), cluster.getMembership());
            }
            case PING -> {
                if (merge(message.getPing().getSender())) {
                    broadcastMembers();
                }
                return ClusterMessages.pong(cluster.getLocalMember());
            }
            case PONG -> {
                if (merge(message.getPong().getSender())) {
                    broadcastMembers();
                }
                return null;
            }
            case MEMBERS -> {
                if (merge(message.getMembers().getMembersList())) {
                    broadcastMembers();
                }
                return null;
            }
            case LEAVE -> {
                var nodeId = message.getLeave().getNodeId();
                cluster.removeMember(nodeId, message.getLeave().getIncarnation());
                System.out.println("[CLUSTER] - " + nodeId + " left, members: " + memberIds());
                broadcastMembers();
                return null;
            }
            default -> {
                return null;
            }
        }
    }

    private void joinSeeds() {
        var join = ClusterMessages.join(cluster.getName(), cluster.getLocalMember());

        for (var seed : seeds) {
            try {
                var response = transport.sendAndReceive(seed, join.toByteArray(), MessageKind.CLUSTER).get();
                var message = ClusterProtocol.ClusterMessage.parseFrom(response);

                if (message.hasWelcome()) {
                    joined.set(true);
                    if (merge(message.getWelcome().getMembersList())) {
                        broadcastMembers();
                    }
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

        cluster.pruneDead(TOMBSTONE_TTL.toMillis());
        broadcastMembers();
    }

    private void heartbeat(Node member) {
        if (!pendingHeartbeats.add(member.id())) {
            return;
        }

        var ping = ClusterMessages.ping(cluster.getLocalMember()).toByteArray();

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
                if (merge(message.getPong().getSender())) {
                    broadcastMembers();
                }
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
        var message = ClusterMessages.members(cluster.getName(), cluster.getMembership()).toByteArray();

        for (var member : cluster.getMembers()) {
            transport.send(new Address(member.host(), member.port()), message, MessageKind.CLUSTER);
        }
    }

    private boolean merge(ClusterProtocol.Node sender) {
        if (sender.getId().isEmpty()) {
            return false;
        }

        return cluster.merge(List.of(ClusterMessages.memberFromProto(sender)));
    }

    private boolean merge(List<ClusterProtocol.Node> members) {
        var observations = members.stream()
            .map(ClusterMessages::memberFromProto)
            .toList();

        return cluster.merge(observations);
    }

    private List<String> memberIds() {
        var ids = new ArrayList<String>();
        ids.add(cluster.getMember().id());
        cluster.getMembers().forEach(member -> ids.add(member.id()));
        return ids;
    }

    private String address(Address address) {
        return address.host() + ":" + address.port();
    }
}
