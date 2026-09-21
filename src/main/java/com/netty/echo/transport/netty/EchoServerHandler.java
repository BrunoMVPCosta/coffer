package com.netty.echo.transport.netty;

import com.google.protobuf.ByteString;
import com.netty.echo.protocol.ClusterProtocol;
import com.netty.echo.protocol.WireProtocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * EchoServerHandler
 */
public class EchoServerHandler extends SimpleChannelInboundHandler<WireProtocol.Envelope> {

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
            var clusterMessage = ClusterProtocol.ClusterMessage.parseFrom(envelope.getPayload());
            System.out.println("[SERVER] - Cluster message: " + clusterMessage.getPayloadCase());
            return;
        }

        System.out.println("[SERVER] - Received: " + envelope.getPayload().toStringUtf8());

        var response = WireProtocol.Envelope.newBuilder()
            .setMessageId(envelope.getMessageId())
            .setKind(WireProtocol.Envelope.Kind.APPLICATION)
            .setPayload(envelope.getPayload())
            .build();

        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
