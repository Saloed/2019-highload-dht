package ru.mail.polis.service.saloed.request.processor.entities;

import com.google.common.collect.Iterators;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.IOExceptionLight;

/**
 * Iterator over response body. todo: replace with something normal.
 */
public class BodyHandlerStub implements
    BodyHandler<Iterator<RecordWithTimestampAndKey>>,
    BodySubscriber<Iterator<RecordWithTimestampAndKey>>,
    Flow.Subscriber<List<ByteBuffer>>,
    Iterator<CompletableFuture<Optional<RecordWithTimestampAndKey>>> {

    private static final int FETCH_SIZE = 128;

    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final Queue<RecordWithTimestampAndKey> records = new ConcurrentLinkedDeque<>();
    private final Queue<CompletableFuture<Optional<RecordWithTimestampAndKey>>> nextOffers
        = new ConcurrentLinkedDeque<>();
    private final CompletableFuture<Void> initialized = new CompletableFuture<>();
    private ResponseInfo responseInfo;
    private ByteBuffer lastReceived = ByteBuffer.allocate(0);
    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
        this.subscription = subscription;
        initialized.complete(null);
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
        while (!nextOffers.isEmpty() && !records.isEmpty()) {
            final var nextRecord = nextOffers.poll();
            final var next = records.poll();
            nextRecord.complete(Optional.of(next));
        }
        if (records.isEmpty()) {
            subscription.request(FETCH_SIZE);
        }
    }

    @Override
    public void onError(final Throwable throwable) {
        nextOffers.iterator().forEachRemaining(it -> it.completeExceptionally(throwable));
        nextOffers.clear();
        finished.set(true);
        initialized.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        finished.set(true);
        if (records.isEmpty()) {
            nextOffers.iterator().forEachRemaining(it -> it.complete(Optional.empty()));
            nextOffers.clear();
        }
    }

    @Override
    public boolean hasNext() {
        return !(finished.get() && records.isEmpty());
    }

    @Override
    public CompletableFuture<Optional<RecordWithTimestampAndKey>> next() {
        final var next = records.poll();
        if (next != null) {
            return CompletableFuture.completedFuture(Optional.of(next));
        }
        final var nextRecord = new CompletableFuture<Optional<RecordWithTimestampAndKey>>();
        nextOffers.add(nextRecord);
        subscription.request(FETCH_SIZE);
        return nextRecord;
    }

    @Override
    public CompletionStage<Iterator<RecordWithTimestampAndKey>> getBody() {
        if (responseInfo == null || responseInfo.statusCode() != 200) {
            return CompletableFuture
                .failedFuture(new IOExceptionLight("Response status is not OK"));
        }
        return initialized.thenApply(__ -> {
            final Iterator<Optional<RecordWithTimestampAndKey>> awaitIterator = Iterators
                .transform(this, future -> {
                    if (future == null) {
                        return Optional.empty();
                    }
                    try {
                        return future.get(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        return Optional.empty();
                    }
                });
            final var nonEmptyIterator = Iterators.filter(awaitIterator, Optional::isPresent);
            return Iterators.transform(nonEmptyIterator, Optional::get);
        });
    }

    @Override
    public BodySubscriber<Iterator<RecordWithTimestampAndKey>> apply(
        final ResponseInfo responseInfo) {
        this.responseInfo = responseInfo;
        return this;
    }
}
