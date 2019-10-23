package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.Nullable;

public final class RecordWithTimestamp {

    private final long timestamp;
    private final RecordType kind;
    private final ByteBuffer value;

    private RecordWithTimestamp(final RecordType kind, final long timestamp,
        final ByteBuffer value) {
        this.timestamp = timestamp;
        this.kind = kind;
        this.value = value;
    }

    /**
     * Performs emptiness check {@link #isValue()} on serialized record {@link #toRawBytes()}.
     *
     * @param raw serialized record
     * @return result of check
     */
    public static boolean recordIsEmpty(@NotNull final byte[] raw) {
        return raw[8] != RecordType.VALUE.value;
    }

    /**
     * Deserialize record from bytes {@link #toRawBytes()}.
     *
     * @param raw bytes
     * @return deserialized record
     */
    public static RecordWithTimestamp fromBytes(@Nullable final byte[] raw) {
        if (raw == null) {
            return new RecordWithTimestamp(RecordType.EMPTY, -1, null);
        }
        final var buffer = ByteBuffer.wrap(raw);
        final var timestamp = buffer.getLong();
        final var kind = RecordType.fromValue(buffer.get());
        return new RecordWithTimestamp(kind, timestamp, buffer);
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
        return new RecordWithTimestamp(RecordType.VALUE, timestamp, value);
    }

    /**
     * Construct empty record.
     *
     * @param timestamp of record
     * @return record
     */
    public static RecordWithTimestamp tombstone(final long timestamp) {
        return new RecordWithTimestamp(RecordType.TOMBSTONE, timestamp, null);
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Check whether record contains some value.
     *
     * @return true if record is {@link RecordType#VALUE} and false otherwise
     */
    public boolean isValue() {
        return kind == RecordType.VALUE;
    }

    /**
     * Check whether record is empty.
     *
     * @return true if record is {@link RecordType#EMPTY} and false otherwise
     */
    public boolean isEmpty() {
        return kind == RecordType.EMPTY;
    }

    /**
     * Retrieve containing value if possible  {@link #isValue()}.
     *
     * @return containing value
     * @throws UnsupportedOperationException if record is empty
     */
    public ByteBuffer getValue() throws UnsupportedOperationException {
        if (!isValue()) {
            throw new UnsupportedOperationException("Empty record has no value");
        }
        return value;
    }

    /**
     * Serialize record to byte array.
     * Format:
     * |   timestamp  | mark (VALUE, TOMBSTONE, EMPTY)   |  value        |
     * | -- 8 bytes --| ------------- 1 byte ------------| 0 - Inf bytes |
     *
     * @return serialized record
     */
    public byte[] toRawBytes() {
        final var valueSize = isValue() ? value.remaining() : 0;
        final var buffer = ByteBuffer
            .allocate(Long.BYTES + 1 + valueSize)
            .putLong(getTimestamp())
            .put(kind.value);
        if (isValue()) {
            buffer.put(value.duplicate());
        }
        return buffer.array();
    }

    private enum RecordType {
        VALUE((byte) 1),
        TOMBSTONE((byte) -1),
        EMPTY((byte) 0);

        final byte value;

        RecordType(final byte value) {
            this.value = value;
        }

        static RecordType fromValue(final byte value) {
            if (value == VALUE.value) {
                return VALUE;
            } else if (value == TOMBSTONE.value) {
                return TOMBSTONE;
            } else {
                return EMPTY;
            }
        }
    }
}
