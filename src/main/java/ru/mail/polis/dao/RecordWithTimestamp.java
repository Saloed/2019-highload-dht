package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public abstract class RecordWithTimestamp {

    static final byte TOMBSTONE = 0;
    static final byte VALUE = 1;

    private final long timestamp;

    RecordWithTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    long getTimestamp() {
        return timestamp;
    }

    abstract boolean isEmpty();

    abstract ByteBuffer getValue();

    abstract byte[] toRawBytes();

    public static boolean recordIsEmpty(@NotNull final byte[] raw) {
        return raw[8] == TOMBSTONE;
    }

    public static RecordWithTimestamp fromBytes(@NotNull final byte[] raw) {
        final var buffer = ByteBuffer.wrap(raw);
        final var timestamp = buffer.getLong();
        final var marker = buffer.get();
        if (marker == TOMBSTONE) {
            return new EmptyRecordWithTimestamp(timestamp);
        }
        return new ValueRecordWithTimestamp(buffer, timestamp);
    }

    public static RecordWithTimestamp fromValue(@NotNull final ByteBuffer value, final long timestamp) {
        return new ValueRecordWithTimestamp(value, timestamp);
    }

    public static RecordWithTimestamp empty(final long timestamp) {
        return new EmptyRecordWithTimestamp(timestamp);
    }
}
