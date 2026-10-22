package io.github.brunomvpcosta.coffer.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class KvCommandParserTest {

    @Test
    void parsesPutWithSpacesInValue() {
        var command = KvCommandParser.parse("put greeting hello world");

        assertTrue(command.hasPut());
        assertEquals("greeting", command.getPut().getKey());
        assertEquals("hello world", command.getPut().getValue().toStringUtf8());
    }

    @Test
    void parsesGetAndDeleteAliases() {
        assertTrue(KvCommandParser.parse("get foo").hasGet());
        assertTrue(KvCommandParser.parse("del foo").hasDelete());
        assertTrue(KvCommandParser.parse("delete foo").hasDelete());
    }

    @Test
    void mapsInfoCommandsToTheInfoMessage() {
        assertTrue(KvCommandParser.parse("info").hasInfo());
        assertTrue(KvCommandParser.parse("status").hasInfo());
        assertTrue(KvCommandParser.parse("keys").hasInfo());
        assertTrue(KvCommandParser.parse("leader").hasInfo());
    }

    @Test
    void rejectsEmptyAndMalformedCommands() {
        assertNull(KvCommandParser.parse(""));
        assertNull(KvCommandParser.parse("   "));
        assertNull(KvCommandParser.parse("put key"));
        assertNull(KvCommandParser.parse("get"));
        assertNull(KvCommandParser.parse("get a b"));
        assertNull(KvCommandParser.parse("del"));
        assertNull(KvCommandParser.parse("unknown"));
    }
}
