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

    /**
     * Performs emptiness check {@link #isEmpty()} on serialized record {@link #toRawBytes()}.
     *
     * @param raw serialized record
     * @return result of check
     */
    public static boolean recordIsEmpty(@NotNull final byte[] raw) {
        return raw[8] == TOMBSTONE;
    }


    /**
     * Deserialize record from bytes {@link #toRawBytes()}.
     *
     * @param raw bytes
     * @return deserialized record
     */
    public static RecordWithTimestamp fromBytes(@NotNull final byte[] raw) {
        final var buffer = ByteBuffer.wrap(raw);
        final var timestamp = buffer.getLong();
        final var marker = buffer.get();
        if (marker == TOMBSTONE) {
            return new EmptyRecordWithTimestamp(timestamp);
        }
        return new ValueRecordWithTimestamp(buffer, timestamp);
    }

    /**
     * Construct record from given value.
     *
     * @param value     of record
     * @param timestamp of record
     * @return record
     */
    public static RecordWithTimestamp fromValue(@NotNull final ByteBuffer value,
        final long timestamp) {
        return new ValueRecordWithTimestamp(value, timestamp);
    }

    /**
     * Construct empty record.
     *
     * @param timestamp of record
     * @return record
     */
    public static RecordWithTimestamp empty(final long timestamp) {
        return new EmptyRecordWithTimestamp(timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Check whether record is a tombstone or contains some value.
     *
     * @return true if record is {@link #TOMBSTONE} and false otherwise
     */
    public abstract boolean isEmpty();

    /**
     * Retrieve containing value if possible (not {@link #isEmpty()}).
     *
     * @return containing value
     * @throws UnsupportedOperationException if record is empty
     */
    public abstract ByteBuffer getValue() throws UnsupportedOperationException;

    /**
     * Serialize record to byte array.
     * Format:
     * |   timestamp  | mark (VALUE or TOMBSTONE) |     value     |
     * | -- 8 bytes --| --------- 1 byte ---------| 0 - Inf bytes |
     *
     * @return serialized record
     */
    public abstract byte[] toRawBytes();
}
