package ru.mail.polis.service.saloed.flow;

import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class IteratorPublisher<T> implements Publisher<T>, Subscription {

    private final Iterator<T> iterator;
    private final AtomicLong requested = new AtomicLong(0);
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private Subscriber<? super T> subscriber;

    public IteratorPublisher(final Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        if (this.subscriber != null) {
            throw new IllegalStateException("Single subscriber publisher already has a subscriber");
        }
        this.subscriber = subscriber;
        this.subscriber.onSubscribe(this);
    }

    @Override
    public void request(final long n) {
        if (n <= 0L) {
            canceled.set(true);
            subscriber.onError(new IllegalArgumentException("Requested amount is below zero"));
            return;
        }
        final long previousRequested = this.requested.getAndAdd(n);
        if (previousRequested == 0L) {
            emit(this.requested.get());
        }
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }

    private void emit(final long requestEmit) {
        long currentRequested = requestEmit;
        int emitted = 0;
        while (true) {
            while (iterator.hasNext() && emitted != currentRequested) {
                if (canceled.get()) {
                    return;
                }
                this.subscriber.onNext(iterator.next());
                emitted++;
            }
            if (!iterator.hasNext()) {
                if (!canceled.get()) {
                    canceled.set(true);
                    this.subscriber.onComplete();
                }
                return;
            }
            final long freshRequested = requested.get();
            if (freshRequested == currentRequested) {
                currentRequested = requested.addAndGet(-currentRequested);
                if (currentRequested == 0L) {
                    break;
                }
                emitted = 0;
            } else {
                currentRequested = freshRequested;
            }
        }

    }
}
