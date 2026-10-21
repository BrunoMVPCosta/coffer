package com.netty.echo.cluster;

import java.util.Collection;
import java.util.List;

import com.netty.echo.protocol.ClusterProtocol;

public final class ClusterMessages {

    public static final int PROTOCOL_VERSION = 2;

    private ClusterMessages() {
    }

    public static Node fromProto(ClusterProtocol.Node node) {
        return new Node(node.getId(), node.getHost(), node.getPort());
    }

    public static Member memberFromProto(ClusterProtocol.Node node) {
        return new Member(
            fromProto(node),
            node.getIncarnation(),
            fromProtoState(node.getState()));
    }

    public static ClusterProtocol.Node toProto(Node node) {
        return toProto(Member.of(node));
    }

    public static ClusterProtocol.Node toProto(Member member) {
        return ClusterProtocol.Node.newBuilder()
            .setId(member.node().id())
            .setHost(member.node().host())
            .setPort(member.node().port())
            .setIncarnation(member.incarnation())
            .setState(toProtoState(member.state()))
            .build();
    }

    public static ClusterProtocol.ClusterMessage join(String clusterName, Node node) {
        return join(clusterName, Member.of(node));
    }

    public static ClusterProtocol.ClusterMessage join(String clusterName, Member member) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setJoin(ClusterProtocol.Join.newBuilder()
                .setClusterName(clusterName)
                .setNode(toProto(member))));
    }

    public static ClusterProtocol.ClusterMessage welcome(String clusterName, Collection<Member> members) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setWelcome(ClusterProtocol.Welcome.newBuilder()
                .setClusterName(clusterName)
                .addAllMembers(toProto(members))));
    }

    public static ClusterProtocol.ClusterMessage ping(Node sender) {
        return ping(Member.of(sender));
    }

    public static ClusterProtocol.ClusterMessage ping(Member sender) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setPing(ClusterProtocol.Ping.newBuilder()
                .setSender(toProto(sender))));
    }

    public static ClusterProtocol.ClusterMessage pong(Node sender) {
        return pong(Member.of(sender));
    }

    public static ClusterProtocol.ClusterMessage pong(Member sender) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setPong(ClusterProtocol.Pong.newBuilder()
                .setSender(toProto(sender))));
    }

    public static ClusterProtocol.ClusterMessage members(String clusterName, Collection<Member> members) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setMembers(ClusterProtocol.Members.newBuilder()
                .setClusterName(clusterName)
                .addAllMembers(toProto(members))));
    }

    public static ClusterProtocol.ClusterMessage leave(Node node) {
        return leave(Member.of(node));
    }

    public static ClusterProtocol.ClusterMessage leave(Member member) {
        return message(ClusterProtocol.ClusterMessage.newBuilder()
            .setLeave(ClusterProtocol.Leave.newBuilder()
                .setNodeId(member.node().id())
                .setIncarnation(member.incarnation())));
    }

    private static MemberState fromProtoState(ClusterProtocol.MemberState state) {
        return switch (state) {
            case MEMBER_STATE_SUSPECT -> MemberState.SUSPECT;
            case MEMBER_STATE_DEAD -> MemberState.DEAD;
            default -> MemberState.ALIVE;
        };
    }

    private static ClusterProtocol.MemberState toProtoState(MemberState state) {
        return switch (state) {
            case SUSPECT -> ClusterProtocol.MemberState.MEMBER_STATE_SUSPECT;
            case DEAD -> ClusterProtocol.MemberState.MEMBER_STATE_DEAD;
            case ALIVE -> ClusterProtocol.MemberState.MEMBER_STATE_ALIVE;
        };
    }

    private static ClusterProtocol.ClusterMessage message(ClusterProtocol.ClusterMessage.Builder builder) {
        return builder.setProtocolVersion(PROTOCOL_VERSION).build();
    }

    private static List<ClusterProtocol.Node> toProto(Collection<Member> members) {
        return members.stream().map(ClusterMessages::toProto).toList();
    }
}
