package ru.mail.polis.service.saloed.request.processor.entity;

import ru.mail.polis.dao.timestamp.RecordWithTimestamp;

/**
 * Represents a result of entity request. May contains record (i.e. for GET request).
 */
public class MaybeRecordWithTimestamp {

    public static final MaybeRecordWithTimestamp EMPTY = new MaybeRecordWithTimestamp(null);
    private final RecordWithTimestamp record;

    public MaybeRecordWithTimestamp(final RecordWithTimestamp record) {
        this.record = record;
    }

    /**
     * Retrieve containing record if exists.
     *
     * @return record
     */
    public RecordWithTimestamp getRecord() {
        if (record == null) {
            throw new IllegalStateException("Record is not present");
        }
        return record;
    }
}
