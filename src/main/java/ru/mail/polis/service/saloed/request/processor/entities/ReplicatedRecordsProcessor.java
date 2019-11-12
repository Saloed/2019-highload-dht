package ru.mail.polis.service.saloed.request.processor.entities;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import ru.mail.polis.Record;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.flow.SingleSubscriberPublisher;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordPayload;

public class ReplicatedRecordsProcessor extends SingleSubscriberPublisher<Payload>
    implements Subscriber<RecordWithTimestampAndKey> {


    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final Queue<RecordWithTimestampAndKey> records = new ConcurrentLinkedQueue<>();
    private Subscription subscription;
    private RecordWithTimestampAndKey previous;

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(RecordWithTimestampAndKey item) {
        if (previous == null) {
            previous = item;
            subscription.request(1);
            return;
        }
        if (previous.sameKeyRecords(item) || previous.isEmpty()) {
            previous = item;
            subscription.request(1);
            return;
        }
        records.add(previous);
        previous = item;
        publish();
    }

    @Override
    public void onError(Throwable throwable) {
        error = throwable;
        publish();
    }

    @Override
    public void onComplete() {
        if (previous != null && !previous.isEmpty()) {
            records.add(previous);
        }
        finished.set(true);
        publish();
    }

    @Override
    public EmitStatus emit() {
        if (records.isEmpty()) {
            return EmitStatus.CANCEL;
        }
        final var next = records.poll();
        if (next == null) {
            return EmitStatus.TRY_AGAIN;
        }
        final var record = Record.of(next.getKey(), next.getValue());
        final var payload = new RecordPayload(record);
        subscriber.onNext(payload);
        subscription.request(1);
        return EmitStatus.SUCCESS;
    }

    @Override
    public boolean isFinished() {
        return finished.get() && records.isEmpty();
    }


}
