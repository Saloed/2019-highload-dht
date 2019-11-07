package ru.mail.polis.service.saloed.request.processor.entities;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Iterator;
import ru.mail.polis.Record;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordPayload;

public class ReplicatedRecordsIterator implements Iterator<Payload> {

    private final PeekingIterator<RecordWithTimestampAndKey> iterator;
    private RecordWithTimestampAndKey nextRecord;

    public ReplicatedRecordsIterator(final Iterator<RecordWithTimestampAndKey> iterator) {
        this.iterator = Iterators.peekingIterator(iterator);
        advance();
    }

    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    private void advance() {
        do {
            nextRecord = advanceRecord();
        }
        while (nextRecord != null && nextRecord.isEmpty());
    }

    private RecordWithTimestampAndKey advanceRecord() {
        RecordWithTimestampAndKey record = null;
        while (iterator.hasNext()) {
            record = iterator.next();
            if (!iterator.hasNext()) {
                break;
            }
            final var next = iterator.peek();
            if (!record.sameKeyRecords(next)) {
                break;
            }
        }
        return record;
    }

    @Override
    public Payload next() {
        final var recordWithTimestampAndKey = nextRecord;
        advance();
        final var record = Record
            .of(recordWithTimestampAndKey.getKey(), recordWithTimestampAndKey.getValue());
        return new RecordPayload(record);
    }
}
