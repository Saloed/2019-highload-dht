package ru.mail.polis.service.saloed.request.processor.entities;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.flow.SingleSubscriberProcessor;

public class RecordsFromBytesProcessor extends
    SingleSubscriberProcessor<List<ByteBuffer>, RecordWithTimestampAndKey> {

    private final AtomicLong requested = new AtomicLong(0);
    private ByteBuffer lastReceived = ByteBuffer.allocate(0);

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
                requested.getAndDecrement();
                subscriber.onNext(record);
            }
            lastReceived = truncate(lastReceived);
        }
        if (requested.get() > 0) {
            source.request(1);
        }
    }

    @Override
    public void request(final long n) {
        final var current = requested.getAndAdd(n);
        if (current == 0L) {
            source.request(1);
        }
    }
}
