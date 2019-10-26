package ru.mail.polis.service.saloed.payload;

import ru.mail.polis.dao.RecordWithTimestampAndKey;

public class RecordWithTimestampAndKeyPayload implements Payload {
    private final RecordWithTimestampAndKey record;

    public RecordWithTimestampAndKeyPayload(final RecordWithTimestampAndKey record) {
        this.record = record;
    }

    @Override
    public byte[] toRawBytes() {
        return record.toRawBytes();
    }
}
