package io.github.brunomvpcosta.coffer.transport.netty;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import io.github.brunomvpcosta.coffer.protocol.WireProtocol;

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
 * RpcClientInitializer
 */
public class RpcClientInitializer extends ChannelInitializer<SocketChannel> {

    static final int MAX_FRAME_LENGTH = 1024 * 1024;

    private final ConcurrentMap<UUID, CompletableFuture<byte[]>> futures;

    public RpcClientInitializer(ConcurrentMap<UUID, CompletableFuture<byte[]>> futures) {
        this.futures = futures;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("protobufDecoder", new ProtobufDecoder(WireProtocol.Envelope.getDefaultInstance()));
        pipeline.addLast("protobufEncoder", new ProtobufEncoder());

        pipeline.addLast("logging", new LoggingHandler(LogLevel.DEBUG));
        pipeline.addLast("handler", new RpcClientHandler(futures));
    }
}
