package ru.mail.polis.service.saloed.flow;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class SingleSubscriberPublisher<T> implements Flow.Publisher<T>, Subscription {

    private final AtomicLong requestedAmount = new AtomicLong(0);
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    protected Subscriber<? super T> subscriber = null;
    protected Throwable error;

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        if (this.subscriber != null) {
            throw new IllegalStateException("Single subscriber publisher already has a subscriber");
        }
        this.subscriber = subscriber;
        this.subscriber.onSubscribe(this);
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            error = new IllegalArgumentException("Incorrect amount requested");
            n = 1;
        }
        final var requested = requestedAmount.getAndAdd(n);
        if(requested == 0L) {
            publish();
        }
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }

    public void publish() {
        while (true) {
            if (canceled.get()) {
                break;
            }
            if (requestedAmount.getAndDecrement() <= 0L) {
                requestedAmount.getAndIncrement();
                break;
            }
            if (error != null) {
                canceled.set(true);
                subscriber.onError(error);
                break;
            }
            if (isFinished()) {
                canceled.set(true);
                subscriber.onComplete();
                break;
            }
            final var status = emit();
            if (status != EmitStatus.SUCCESS) {
                requestedAmount.getAndIncrement();
                if (status == EmitStatus.CANCEL) {
                    break;
                }
            }
        }
    }

    protected abstract EmitStatus emit();

    protected abstract boolean isFinished();

    public enum EmitStatus {
        SUCCESS, CANCEL, TRY_AGAIN
    }

}
