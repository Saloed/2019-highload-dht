package ru.mail.polis.dao.timestamp;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
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
        final var keyLength = buffer.getInt();
        final var valueLength = buffer.getInt();
        final var key = new byte[keyLength];
        final var value = new byte[valueLength];
        buffer.get(key);
        buffer.get(value);
        final var valueRecord = RecordWithTimestamp.fromBytes(value);
        return new RecordWithTimestampAndKey(valueRecord, ByteBuffer.wrap(key));
    }

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

    public boolean sameKeyRecords(final RecordWithTimestampAndKey other) {
        return key.equals(other.key);
    }

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

}
