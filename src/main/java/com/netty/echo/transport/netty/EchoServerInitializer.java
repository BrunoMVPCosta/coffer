package com.netty.echo.transport.netty;

import com.netty.echo.protocol.WireProtocol;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * EchoServerInitializer
 */
public class EchoServerInitializer extends ChannelInitializer<SocketChannel> {

    static final int MAX_FRAME_LENGTH = 1024 * 1024;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("protobufDecoder", new ProtobufDecoder(WireProtocol.Envelope.getDefaultInstance()));
        pipeline.addLast("protobufEncoder", new ProtobufEncoder());

        pipeline.addLast("handler", new EchoServerHandler());

        pipeline.addLast("logging", new LoggingHandler(LogLevel.DEBUG));
    }
}
