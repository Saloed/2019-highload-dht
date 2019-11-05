package ru.mail.polis.service.saloed.payload;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import ru.mail.polis.Record;
import ru.mail.polis.dao.ByteBufferUtils;

public class RecordPayload implements Payload {

    private static final byte[] DELIMITER = "\n".getBytes(StandardCharsets.UTF_8);
    private final Record record;

    public RecordPayload(final Record record) {
        this.record = record;
    }

    @Override
    public byte[] toRawBytes() {
        final var key = ByteBufferUtils.toArray(record.getKey());
        final var value = ByteBufferUtils.toArray(record.getValue());
        return ByteBuffer.allocate(key.length + DELIMITER.length + value.length)
            .put(key)
            .put(DELIMITER)
            .put(value)
            .array();
    }
}
