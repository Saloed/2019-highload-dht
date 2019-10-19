package ru.mail.polis.dao;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class RecordWithTimestamp {

    private enum RecordType {
        TOMBSTONE(Byte.MIN_VALUE),
        VALUE(Byte.MAX_VALUE);

        public final byte representation;

        RecordType(byte representation) {
            this.representation = representation;
        }
    }

    private static final byte[] TOMBSTONE_MARK = {RecordType.TOMBSTONE.representation};
    private static final byte[] VALUE_MARK = {RecordType.VALUE.representation};
    private static final byte[] EMPTY_VALUE = {};

    private final byte[] key;
    private final byte[] value;
    private final RecordType type;
    private final long timestamp;


    private RecordWithTimestamp(final byte[] key, final RecordType type, final byte[] value, final long timestamp) {
        this.key = key;
        this.value = value;
        this.type = type;
        this.timestamp = timestamp;
    }

    byte[] getKey() {
        return key;
    }

    long getTimestamp() {
        return timestamp;
    }


    byte[] getValue() {
        return value;
    }


    byte[] toRawBytes() {
        final var mark = type == RecordType.VALUE ? VALUE_MARK : TOMBSTONE_MARK;
        final var timestampBytes = Longs.toByteArray(this.timestamp);
        return Bytes.concat(mark, timestampBytes, value);
    }


    boolean isEmpty() {
        return this.type == RecordType.TOMBSTONE;
    }

    @Nullable
    Record withoutTimestamp() {
        if (isEmpty()) return null;
        final var key = ByteBuffer.wrap(this.key);
        final var value = ByteBuffer.wrap(this.value);
        return Record.of(key, value);
    }

    public static RecordWithTimestamp fromRawBytes(@NotNull final byte[] key, @NotNull final byte[] raw) {
        final var type = raw[0] == RecordType.VALUE.representation ? RecordType.VALUE : RecordType.TOMBSTONE;
        final var timestampArray = Arrays.copyOfRange(raw, 1, 9);
        final var timestamp = Longs.fromByteArray(timestampArray);
        final var value = type == RecordType.VALUE ? Arrays.copyOfRange(raw, 9, raw.length) : EMPTY_VALUE;
        return new RecordWithTimestamp(key, type, value, timestamp);
    }

    public static RecordWithTimestamp empty(@NotNull final byte[] key, final long timestamp) {
        return new RecordWithTimestamp(key, RecordType.TOMBSTONE, EMPTY_VALUE, timestamp);
    }

    public static RecordWithTimestamp create(@NotNull final byte[] key, @NotNull final byte[] value, final long timestamp) {
        return new RecordWithTimestamp(key, RecordType.VALUE, value, timestamp);
    }

    public static RecordWithTimestamp empty(@NotNull final ByteBuffer key) {
        final var timestamp = System.currentTimeMillis();
        final var keyArray = ByteBufferUtils.toArray(key);
        return empty(keyArray, timestamp);
    }

    public static RecordWithTimestamp create(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final var timestamp = System.currentTimeMillis();
        final var keyArray = ByteBufferUtils.toArray(key);
        final var valueArray = ByteBufferUtils.toArray(value);
        return create(keyArray, valueArray, timestamp);
    }


}
