package com.netty.echo.net;

public record Address(String host, int port) {

    private static final String SPLIT_IPv4 = ":";
    private static final String SPLIT_IPv6 = "]:";

    /**
     * Returns a new address instance given an address string
     * @param address the address string with the format host:port
     * @return the new address instance
     */
    public static Address from(String address) {

        if(address == null || address.isBlank()) {
            throw new IllegalArgumentException("Address cannot be empty or null");
        }

        String splitted = SPLIT_IPv4;

        if(address.startsWith("[")) {
            address = address.substring(1);
            splitted = SPLIT_IPv6;
        }

        String[] hostAndPort = address.split(splitted);
        if(hostAndPort.length != 2) {
            throw new MalformedAddressException(String.format("The address %s is not valid", address));
        }

        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);

        return new Address(host, port);
    }
}
