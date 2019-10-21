package ru.mail.polis.dao;

import java.nio.ByteBuffer;

public class ValueRecordWithTimestamp extends RecordWithTimestamp {

    private final ByteBuffer value;

    ValueRecordWithTimestamp(final ByteBuffer value, final long timestamp) {
        super(timestamp);
        this.value = value;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public ByteBuffer getValue() throws UnsupportedOperationException {
        return value;
    }

    @Override
    public byte[] toRawBytes() {
        return ByteBuffer
            .allocate(Long.BYTES + 1 + value.remaining())
            .putLong(getTimestamp())
            .put(VALUE)
            .put(value.duplicate())
            .array();
    }
}
