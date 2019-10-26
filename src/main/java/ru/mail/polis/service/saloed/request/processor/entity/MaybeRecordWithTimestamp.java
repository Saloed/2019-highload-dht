package ru.mail.polis.service.saloed.request.processor.entity;

import ru.mail.polis.dao.RecordWithTimestamp;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;

public class MaybeRecordWithTimestamp {

    public static final MaybeRecordWithTimestamp EMPTY = new MaybeRecordWithTimestamp(null);
    private final RecordWithTimestamp record;

    public MaybeRecordWithTimestamp(final RecordWithTimestamp record) {
        this.record = record;
    }

    public RecordWithTimestamp getRecord() {
        if (record == null) {
            throw new IllegalStateException("Record is not present");
        }
        return record;
    }
}
