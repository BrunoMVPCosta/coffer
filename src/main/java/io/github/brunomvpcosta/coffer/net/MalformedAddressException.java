package io.github.brunomvpcosta.coffer.net;

/**
 * MailformedAddressException
 */
public class MalformedAddressException extends RuntimeException {

    private static final long serialVersionUID = -3008255543968635420L;

    public MalformedAddressException() {
        super();
    }

    public MalformedAddressException(String message) {
        super(message);
    }

    public MalformedAddressException(String message, Throwable cause) {
        super(message, cause);
    }
}