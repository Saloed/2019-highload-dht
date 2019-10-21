package ru.mail.polis.dao;

import java.nio.ByteBuffer;

public class EmptyRecordWithTimestamp extends RecordWithTimestamp {

    EmptyRecordWithTimestamp(final long timestamp) {
        super(timestamp);
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public ByteBuffer getValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Empty record has no value");
    }

    @Override
    public byte[] toRawBytes() {
        return ByteBuffer
            .allocate(Long.BYTES + 1)
            .putLong(getTimestamp())
            .put(TOMBSTONE)
            .array();
    }
}
