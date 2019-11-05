package ru.mail.polis.dao.timestamp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.ByteBufferUtils;

public final class RecordWithTimestampAndKey implements Comparable<RecordWithTimestampAndKey> {

    private final RecordWithTimestamp value;
    private final ByteBuffer key;

    private RecordWithTimestampAndKey(final RecordWithTimestamp value, final ByteBuffer key) {
        this.value = value;
        this.key = key;
    }

    /**
     * Deserialize record from bytes.
     *
     * @param bytes serialized record
     * @return record
     */
    public static RecordWithTimestampAndKey fromRawBytes(final byte[] bytes) {
        final var buffer = ByteBuffer.wrap(bytes);
        return fromRawBytes(buffer);
    }

    public static boolean mayDeserialize(final ByteBuffer buffer) {
        if (buffer.remaining() < Integer.BYTES * 2) {
            return false;
        }
        final var copy = buffer.duplicate();
        final var keyLength = copy.getInt();
        final var valueLength = copy.getInt();
        return copy.remaining() >= keyLength + valueLength;
    }

    public static RecordWithTimestampAndKey fromRawBytes(final ByteBuffer buffer) {
        final var keyLength = buffer.getInt();
        final var valueLength = buffer.getInt();
        final var key = new byte[keyLength];
        final var value = new byte[valueLength];
        buffer.get(key);
        buffer.get(value);
        final var valueRecord = RecordWithTimestamp.fromBytes(value);
        return new RecordWithTimestampAndKey(valueRecord, ByteBuffer.wrap(key));
    }

    /**
     * Creates record with key, from record and key.
     *
     * @param key   of record
     * @param value record with timestamp
     * @return created record
     */
    public static RecordWithTimestampAndKey fromKeyValue(final ByteBuffer key,
        final RecordWithTimestamp value) {
        return new RecordWithTimestampAndKey(value, key);
    }

    @Override
    public int compareTo(@NotNull final RecordWithTimestampAndKey other) {
        final var keyComparison = key.compareTo(other.key);
        return keyComparison == 0 ? Long.compare(value.getTimestamp(), other.value.getTimestamp())
            : keyComparison;
    }

    public boolean isEmpty() {
        return !value.isValue();
    }

    public ByteBuffer getValue() {
        return value.getValue();
    }

    public ByteBuffer getKey() {
        return key;
    }

    /**
     * Compare two records by key.
     *
     * @param other record
     * @return true if keys are equal
     */
    public boolean sameKeyRecords(final RecordWithTimestampAndKey other) {
        return key.equals(other.key);
    }

    /**
     * Serialize record to bytes.
     *
     * @return bytes
     */
    public byte[] toRawBytes() {
        final var valueBytes = value.toRawBytes();
        final var keyBytes = ByteBufferUtils.toArray(key);
        return ByteBuffer
            .allocate(Integer.BYTES + Integer.BYTES + keyBytes.length + valueBytes.length)
            .putInt(keyBytes.length)
            .putInt(valueBytes.length)
            .put(keyBytes)
            .put(valueBytes)
            .array();
    }

    @Override
    public String toString() {
        final var keyBytes = ByteBufferUtils.toArray(key.duplicate());
        return new String(keyBytes, StandardCharsets.UTF_8)
            + " "
            + value.toString();
    }
}
