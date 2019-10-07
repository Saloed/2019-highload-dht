package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ByteBufferUtils {

    /**
     * Retrieve array from a {@link java.nio.ByteBuffer}
     *
     * @param buffer -- byte buffer to extract from
     * @return array
     */
    public static byte[] toArray(@NotNull final ByteBuffer buffer) {
        final var bufferCopy = buffer.duplicate();
        final var array = new byte[bufferCopy.remaining()];
        bufferCopy.get(array);
        return array;
    }

}
