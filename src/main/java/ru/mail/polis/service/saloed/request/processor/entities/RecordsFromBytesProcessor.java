package ru.mail.polis.service.saloed.request.processor.entities;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.atomic.AtomicBoolean;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.flow.SingleSubscriberPublisher;

public class RecordsFromBytesProcessor extends SingleSubscriberPublisher<RecordWithTimestampAndKey>
    implements Processor<List<ByteBuffer>, RecordWithTimestampAndKey> {

    private static final int FETCH_SIZE = 32;

    private final Queue<RecordWithTimestampAndKey> records = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private ByteBuffer lastReceived = ByteBuffer.allocate(0);
    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
        this.subscription = subscription;
    }

    private ByteBuffer extend(final ByteBuffer buffer, final ByteBuffer other) {
        return ByteBuffer.allocate(buffer.remaining() + other.remaining())
            .put(buffer)
            .put(other)
            .rewind();
    }

    private ByteBuffer truncate(final ByteBuffer buffer) {
        if (buffer.remaining() == 0) {
            return ByteBuffer.allocate(0);
        }
        final var array = ByteBufferUtils.toArray(buffer);
        return ByteBuffer.wrap(array);
    }

    @Override
    public void onNext(final List<ByteBuffer> item) {
        for (final var buffer : item) {
            lastReceived = extend(lastReceived, buffer);
            while (RecordWithTimestampAndKey.mayDeserialize(lastReceived)) {
                final var record = RecordWithTimestampAndKey.fromRawBytes(lastReceived);
                records.add(record);
            }
            lastReceived = truncate(lastReceived);
        }
        if (records.isEmpty()) {
            subscription.request(FETCH_SIZE);
            return;
        }
        publish();
    }

    @Override
    public void onError(final Throwable throwable) {
        error = throwable;
        publish();
    }

    @Override
    public void onComplete() {
        finished.set(true);
    }

    @Override
    protected EmitStatus emit() {
        if (records.isEmpty()) {
            subscription.request(FETCH_SIZE);
            return EmitStatus.CANCEL;
        }
        final var next = records.poll();
        if (next == null) {
            return EmitStatus.TRY_AGAIN;
        }
        subscriber.onNext(next);
        return EmitStatus.SUCCESS;
    }

    @Override
    protected boolean isFinished() {
        return finished.get() && records.isEmpty();
    }
}
