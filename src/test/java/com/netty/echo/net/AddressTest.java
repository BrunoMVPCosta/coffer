package com.netty.echo.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * AddressTest
 */
class AddressTest {

    @Test
    void fromIPv4LocalAddress() {
        var address = Address.from("127.0.0.1:8080");

        assertEquals("127.0.0.1", address.host());
        assertEquals(8080, address.port());
    }

    @Test
    void fromIPv4Address() {
        var address = Address.from("216.58.211.36:80");

        assertEquals("216.58.211.36", address.host());
        assertEquals(80, address.port());
    }

    @Test
    void fromIPv6LocalAddress() {
        var address = Address.from("[::1]:80");

        assertEquals("::1", address.host());
        assertEquals(80, address.port());
    }

    @Test
    void fromIPv6Address() {
        var address = Address.from("[2a03:2880:f129:83:face:b00c::25de]:80");

        assertEquals("2a03:2880:f129:83:face:b00c::25de", address.host());
        assertEquals(80, address.port());
    }

    @Test
    void fromEmptyAddressShouldThrowIllegalArgumentException() {
        var exception = assertThrows(IllegalArgumentException.class, () -> Address.from(""));

        assertEquals("Address cannot be empty or null", exception.getMessage());
    }

    @Test
    void fromInvalidAddressShouldThrowMalformedAddressException() {
        var address = "12.2.3.4.2";
        var exception = assertThrows(MalformedAddressException.class, () -> Address.from(address));

        assertEquals("The address %s is not valid".formatted(address), exception.getMessage());
    }

    @Test
    void twoIdenticalInstancesShouldBeEqual() {
        var address1 = Address.from("127.0.0.1:80");
        var address2 = Address.from("127.0.0.1:80");

        assertEquals(address1, address2);
    }

    @Test
    void twoDifferentInstancesShouldNotBeEqual() {
        var address1 = Address.from("127.0.0.1:80");
        var address2 = Address.from("127.0.0.1:81");

        assertFalse(address1.equals(address2));
    }
}
