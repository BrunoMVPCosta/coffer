package com.netty.echo.kv;

import com.google.protobuf.ByteString;
import com.netty.echo.protocol.KvProtocol;

/**
 * Parses shell and command-line input into KV commands.
 */
public final class KvCommandParser {

    private KvCommandParser() {
    }

    /**
     * @return the command, or null when the line is empty or invalid
     */
    public static KvProtocol.KvCommand parse(String line) {
        if (line == null) {
            return null;
        }

        var trimmed = line.trim();

        if (trimmed.isEmpty()) {
            return null;
        }

        var parts = trimmed.split("\\s+", 3);

        return switch (parts[0].toLowerCase()) {
            case "put" -> {
                if (parts.length < 3 || parts[1].isBlank()) {
                    yield null;
                }

                yield KvProtocol.KvCommand.newBuilder()
                    .setPut(KvProtocol.Put.newBuilder()
                        .setKey(parts[1])
                        .setValue(ByteString.copyFromUtf8(parts[2])))
                    .build();
            }
            case "get" -> {
                if (parts.length != 2 || parts[1].isBlank()) {
                    yield null;
                }

                yield KvProtocol.KvCommand.newBuilder()
                    .setGet(KvProtocol.Get.newBuilder().setKey(parts[1]))
                    .build();
            }
            case "delete", "del" -> {
                if (parts.length != 2 || parts[1].isBlank()) {
                    yield null;
                }

                yield KvProtocol.KvCommand.newBuilder()
                    .setDelete(KvProtocol.Delete.newBuilder().setKey(parts[1]))
                    .build();
            }
            case "info", "status", "keys", "leader" -> KvProtocol.KvCommand.newBuilder()
                .setInfo(KvProtocol.Info.getDefaultInstance())
                .build();
            default -> null;
        };
    }
}
