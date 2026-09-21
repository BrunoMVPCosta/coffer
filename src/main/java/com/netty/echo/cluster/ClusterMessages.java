package com.netty.echo.cluster;

import java.util.Collection;
import java.util.List;

import com.netty.echo.protocol.ClusterProtocol;

public final class ClusterMessages {

    public static final int PROTOCOL_VERSION = 1;

    private ClusterMessages() {
    }

    public static Node fromProto(ClusterProtocol.Node node) {
        return new Node(node.getId(), node.getHost(), node.getPort());
    }

    public static ClusterProtocol.Node toProto(Node node) {
        return ClusterProtocol.Node.newBuilder()
            .setId(node.id())
            .setHost(node.host())
            .setPort(node.port())
            .build();
    }

    public static ClusterProtocol.ClusterMessage join(String clusterName, Node node) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setJoin(ClusterProtocol.Join.newBuilder()
                .setClusterName(clusterName)
                .setNode(toProto(node))));
    }

    public static ClusterProtocol.ClusterMessage welcome(String clusterName, Collection<Node> members) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setWelcome(ClusterProtocol.Welcome.newBuilder()
                .setClusterName(clusterName)
                .addAllMembers(toProto(members))));
    }

    public static ClusterProtocol.ClusterMessage ping(Node sender) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setPing(ClusterProtocol.Ping.newBuilder()
                .setSender(toProto(sender))));
    }

    public static ClusterProtocol.ClusterMessage pong(Node sender) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setPong(ClusterProtocol.Pong.newBuilder()
                .setSender(toProto(sender))));
    }

    public static ClusterProtocol.ClusterMessage members(String clusterName, Collection<Node> members) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setMembers(ClusterProtocol.Members.newBuilder()
                .setClusterName(clusterName)
                .addAllMembers(toProto(members))));
    }

    public static ClusterProtocol.ClusterMessage leave(String nodeId) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setLeave(ClusterProtocol.Leave.newBuilder()
                .setNodeId(nodeId)));
    }

    private static ClusterProtocol.ClusterMessage message(ClusterProtocol.ClusterMessage.Builder builder) {
        return builder.setProtocolVersion(PROTOCOL_VERSION).build();
    }

    private static List<ClusterProtocol.Node> toProto(Collection<Node> nodes) {
        return nodes.stream().map(ClusterMessages::toProto).toList();
    }
}
