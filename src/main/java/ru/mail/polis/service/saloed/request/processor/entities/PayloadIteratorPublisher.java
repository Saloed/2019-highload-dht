package ru.mail.polis.service.saloed.request.processor.entities;

import java.util.Iterator;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import ru.mail.polis.service.saloed.payload.Payload;

public final class PayloadIteratorPublisher extends SingleSubscriberPublisher<Payload> implements
    Flow.Subscription {

    private final Iterator<Payload> iterator;
    private final AtomicLong requestedAmount;
    private final AtomicBoolean canceled;
    private Throwable error;

    public PayloadIteratorPublisher(final Iterator<Payload> iterator) {
        this.iterator = iterator;
        this.requestedAmount = new AtomicLong(0);
        this.canceled = new AtomicBoolean(false);
    }

    @Override
    Subscription getSubscription() {
        return this;
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            error = new IllegalArgumentException("Incorrect amount requested");
            n = 1;
        }
        final var requested = requestedAmount.get();
        final var update = requestedAmount.addAndGet(n);
        if (requested == 0) {
            emit(update);
        }
    }

    private void emit(long currentRequested) {
        long emitted = 0;
        while (true) {
            if (error != null) {
                canceled.set(true);
                subscriber.onError(error);
                return;
            }

            while (iterator.hasNext() && emitted != currentRequested) {
                if (canceled.get()) {
                    return;
                }
                final var next = iterator.next();
                subscriber.onNext(next);
                emitted++;
            }

            if (!iterator.hasNext()) {
                if (!canceled.get()) {
                    canceled.set(true);
                    subscriber.onComplete();
                }
                return;
            }

            long freshRequested = requestedAmount.get();
            if (freshRequested == currentRequested) {
                currentRequested = requestedAmount.addAndGet(-currentRequested);
                if (currentRequested == 0L) {
                    break;
                }
                emitted = 0;
            } else {
                currentRequested = freshRequested;
            }
        }
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }
}
