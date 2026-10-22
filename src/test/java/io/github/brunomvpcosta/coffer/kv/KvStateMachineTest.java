package io.github.brunomvpcosta.coffer.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.protobuf.ByteString;
import io.github.brunomvpcosta.coffer.protocol.KvProtocol;

import org.junit.jupiter.api.Test;

class KvStateMachineTest {

    private final KvStateMachine state = new KvStateMachine();

    @Test
    void putGetAndDelete() {
        state.apply(put("a", "1"));

        assertArrayEquals(bytes("1"), state.get("a").orElseThrow());

        state.apply(delete("a"));

        assertTrue(state.get("a").isEmpty());
        assertEquals(0, state.size());
    }

    @Test
    void ignoresReadsAndNoOps() {
        state.apply(get("a"));
        state.apply(KvProtocol.KvCommand.getDefaultInstance());

        assertEquals(0, state.size());
    }

    @Test
    void listsKeysSorted() {
        state.apply(put("b", "2"));
        state.apply(put("a", "1"));

        assertEquals(List.of("a", "b"), state.keys());
    }

    @Test
    void snapshotRoundTripsState() {
        state.apply(put("a", "1"));
        state.apply(put("b", "2"));

        var restored = new KvStateMachine();
        restored.restore(state.snapshot());

        assertArrayEquals(bytes("1"), restored.get("a").orElseThrow());
        assertArrayEquals(bytes("2"), restored.get("b").orElseThrow());
        assertEquals(List.of("a", "b"), restored.keys());
    }

    private static KvProtocol.KvCommand put(String key, String value) {
        return KvProtocol.KvCommand.newBuilder()
            .setPut(KvProtocol.Put.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(bytes(value))))
            .build();
    }

    private static KvProtocol.KvCommand get(String key) {
        return KvProtocol.KvCommand.newBuilder()
            .setGet(KvProtocol.Get.newBuilder().setKey(key))
            .build();
    }

    private static KvProtocol.KvCommand delete(String key) {
        return KvProtocol.KvCommand.newBuilder()
            .setDelete(KvProtocol.Delete.newBuilder().setKey(key))
            .build();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
