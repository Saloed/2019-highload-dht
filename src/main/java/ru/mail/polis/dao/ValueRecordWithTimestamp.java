package ru.mail.polis.dao;

import java.nio.ByteBuffer;

public class ValueRecordWithTimestamp extends RecordWithTimestamp {
    private final ByteBuffer value;

    ValueRecordWithTimestamp(final ByteBuffer value, final long timestamp) {
        super(timestamp);
        this.value = value;
    }

    @Override
    boolean isEmpty() {
        return false;
    }

    @Override
    ByteBuffer getValue() {
        return value;
    }

    @Override
    byte[] toRawBytes() {
        final var buffer = ByteBuffer.allocate(Long.BYTES + 1 + value.remaining());
        buffer.putLong(getTimestamp());
        buffer.put(VALUE);
        buffer.put(value.duplicate());
        return buffer.array();
    }
}
