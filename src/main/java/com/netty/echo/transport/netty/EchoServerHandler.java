package com.netty.echo.transport.netty;

import com.google.protobuf.ByteString;
import com.netty.echo.cluster.ClusterMessageHandler;
import com.netty.echo.protocol.ClusterProtocol;
import com.netty.echo.protocol.RaftProtocol;
import com.netty.echo.protocol.WireProtocol;
import com.netty.echo.raft.RaftMessageHandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * EchoServerHandler
 */
public class EchoServerHandler extends SimpleChannelInboundHandler<WireProtocol.Envelope> {

    private final ClusterMessageHandler clusterMessageHandler;
    private final RaftMessageHandler raftMessageHandler;

    public EchoServerHandler(ClusterMessageHandler clusterMessageHandler, RaftMessageHandler raftMessageHandler) {
        this.clusterMessageHandler = clusterMessageHandler;
        this.raftMessageHandler = raftMessageHandler;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[SERVER] - " + ctx.channel().remoteAddress() + " has joined!\n");

        var notice = WireProtocol.Envelope.newBuilder()
            .setKind(WireProtocol.Envelope.Kind.APPLICATION)
            .setPayload(ByteString.copyFromUtf8("[SERVER] - " + ctx.channel().remoteAddress() + " has joined!"))
            .build();

        ctx.writeAndFlush(notice);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[SERVER] - " + ctx.channel().remoteAddress() + " has left!\n");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, WireProtocol.Envelope envelope) throws Exception {
        if (envelope.getKind() == WireProtocol.Envelope.Kind.CLUSTER) {
            handleClusterMessage(ctx, envelope);
            return;
        }

        if (envelope.getKind() == WireProtocol.Envelope.Kind.RAFT) {
            handleRaftMessage(ctx, envelope);
            return;
        }

        System.out.println("[SERVER] - Received: " + envelope.getPayload().toStringUtf8());

        if (envelope.getMessageId().isEmpty()) {
            return;
        }

        respond(ctx, envelope, WireProtocol.Envelope.Kind.APPLICATION, envelope.getPayload());
    }

    private void handleClusterMessage(ChannelHandlerContext ctx, WireProtocol.Envelope envelope) throws Exception {
        var message = ClusterProtocol.ClusterMessage.parseFrom(envelope.getPayload());
        var response = clusterMessageHandler.handle(message);

        if (response == null || envelope.getMessageId().isEmpty()) {
            return;
        }

        respond(ctx, envelope, WireProtocol.Envelope.Kind.CLUSTER, response.toByteString());
    }

    private void handleRaftMessage(ChannelHandlerContext ctx, WireProtocol.Envelope envelope) throws Exception {
        var message = RaftProtocol.RaftMessage.parseFrom(envelope.getPayload());
        var response = raftMessageHandler.handle(message);

        if (response == null || envelope.getMessageId().isEmpty()) {
            return;
        }

        respond(ctx, envelope, WireProtocol.Envelope.Kind.RAFT, response.toByteString());
    }

    private void respond(ChannelHandlerContext ctx, WireProtocol.Envelope request, WireProtocol.Envelope.Kind kind, ByteString payload) {
        var response = WireProtocol.Envelope.newBuilder()
            .setMessageId(request.getMessageId())
            .setKind(kind)
            .setPayload(payload)
            .build();

        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
