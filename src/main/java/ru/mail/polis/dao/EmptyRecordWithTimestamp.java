package ru.mail.polis.dao;

import java.nio.ByteBuffer;

public class EmptyRecordWithTimestamp extends RecordWithTimestamp {
    EmptyRecordWithTimestamp(long timestamp) {
        super(timestamp);
    }

    @Override
    boolean isEmpty() {
        return true;
    }

    @Override
    ByteBuffer getValue() {
        throw new UnsupportedOperationException("Empty record has no value");
    }

    @Override
    byte[] toRawBytes() {
        final var buffer = ByteBuffer.allocate(Long.BYTES + 1);
        buffer.putLong(getTimestamp());
        buffer.put(TOMBSTONE);
        return buffer.array();
    }
}
