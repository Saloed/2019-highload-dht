package ru.mail.polis.service.saloed.request.processor.entities;

import ru.mail.polis.Record;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.flow.SingleSubscriberProcessor;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordPayload;

public class ReplicatedRecordsProcessor extends
    SingleSubscriberProcessor<RecordWithTimestampAndKey, Payload> {

    private RecordWithTimestampAndKey previous;


    @Override
    public void onNext(final RecordWithTimestampAndKey item) {
        if (previous == null) {
            previous = item;
            request(1);
            return;
        }
        if (previous.sameKeyRecords(item) || previous.isEmpty()) {
            previous = item;
            request(1);
            return;
        }
        final var record = previous;
        previous = item;
        pushNext(record);
    }

    private void pushNext(final RecordWithTimestampAndKey next) {
        final var record = Record.of(next.getKey(), next.getValue());
        final var payload = new RecordPayload(record);
        onNextResult(payload);
    }

    @Override
    public void onComplete() {
        if (previous != null && !previous.isEmpty()) {
            pushNext(previous);
        }
        super.onComplete();
    }
}
