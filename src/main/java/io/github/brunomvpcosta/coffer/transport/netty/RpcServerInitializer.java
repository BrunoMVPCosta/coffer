package io.github.brunomvpcosta.coffer.transport.netty;

import io.github.brunomvpcosta.coffer.cluster.ClusterMessageHandler;
import io.github.brunomvpcosta.coffer.kv.KvMessageHandler;
import io.github.brunomvpcosta.coffer.protocol.WireProtocol;
import io.github.brunomvpcosta.coffer.raft.RaftMessageHandler;

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
 * RpcServerInitializer
 */
public class RpcServerInitializer extends ChannelInitializer<SocketChannel> {

    static final int MAX_FRAME_LENGTH = 1024 * 1024;

    private final ClusterMessageHandler clusterMessageHandler;
    private final RaftMessageHandler raftMessageHandler;
    private final KvMessageHandler kvMessageHandler;

    public RpcServerInitializer(
        ClusterMessageHandler clusterMessageHandler,
        RaftMessageHandler raftMessageHandler,
        KvMessageHandler kvMessageHandler) {

        this.clusterMessageHandler = clusterMessageHandler;
        this.raftMessageHandler = raftMessageHandler;
        this.kvMessageHandler = kvMessageHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("protobufDecoder", new ProtobufDecoder(WireProtocol.Envelope.getDefaultInstance()));
        pipeline.addLast("protobufEncoder", new ProtobufEncoder());

        pipeline.addLast("handler", new RpcServerHandler(
            clusterMessageHandler, raftMessageHandler, kvMessageHandler));

        pipeline.addLast("logging", new LoggingHandler(LogLevel.DEBUG));
    }
}
