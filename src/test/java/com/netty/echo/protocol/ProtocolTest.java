package com.netty.echo.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.google.protobuf.ByteString;

import org.junit.jupiter.api.Test;

/**
 * ProtocolTest
 */
class ProtocolTest {

    @Test
    void clusterMessageRoundTrip() throws Exception {
        var node = ClusterProtocol.Node.newBuilder()
            .setId("node-1")
            .setHost("127.0.0.1")
            .setPort(8080)
            .build();

        var message = ClusterProtocol.ClusterMessage.newBuilder()
            .setProtocolVersion(1)
            .setJoin(ClusterProtocol.Join.newBuilder()
                .setClusterName("netty-cluster")
                .setNode(node))
            .build();

        var parsed = ClusterProtocol.ClusterMessage.parseFrom(message.toByteArray());

        assertTrue(parsed.hasJoin());
        assertEquals(1, parsed.getProtocolVersion());
        assertEquals("netty-cluster", parsed.getJoin().getClusterName());
        assertEquals("node-1", parsed.getJoin().getNode().getId());
        assertEquals(8080, parsed.getJoin().getNode().getPort());
    }

    @Test
    void envelopeRoundTrip() throws Exception {
        var messageId = UUID.randomUUID().toString();
        var payload = "hello".getBytes(StandardCharsets.UTF_8);

        var envelope = WireProtocol.Envelope.newBuilder()
            .setMessageId(messageId)
            .setKind(WireProtocol.Envelope.Kind.APPLICATION)
            .setPayload(ByteString.copyFrom(payload))
            .build();

        var parsed = WireProtocol.Envelope.parseFrom(envelope.toByteArray());

        assertEquals(messageId, parsed.getMessageId());
        assertEquals(WireProtocol.Envelope.Kind.APPLICATION, parsed.getKind());
        assertArrayEquals(payload, parsed.getPayload().toByteArray());
    }
}
