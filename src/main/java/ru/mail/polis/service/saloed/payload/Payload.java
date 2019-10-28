package ru.mail.polis.service.saloed.payload;

/**
 * Payload for @link{{@link ru.mail.polis.service.saloed.StreamHttpSession}}.
 */
public interface Payload {

    /**
     * Serialize data to byte sequence.
     *
     * @return bytes
     */
    byte[] toRawBytes();
}
