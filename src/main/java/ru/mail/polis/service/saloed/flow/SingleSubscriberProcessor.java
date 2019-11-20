package ru.mail.polis.service.saloed.flow;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

public abstract class SingleSubscriberProcessor<T, R> implements Processor<T, R>, Subscription {

    private Subscriber<? super R> subscriber;
    private Flow.Subscription source;

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
        this.source = subscription;
        subscribeWhenReady();
    }

    @Override
    public void subscribe(final Subscriber<? super R> subscriber) {
        if (this.subscriber != null) {
            throw new IllegalStateException("Single subscriber publisher already has a subscriber");
        }
        this.subscriber = subscriber;
        subscribeWhenReady();
    }

    private void subscribeWhenReady() {
        if (this.source != null && this.subscriber != null) {
            this.subscriber.onSubscribe(this);
        }
    }

    public void onNextResult(final R item) {
        subscriber.onNext(item);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }

    @Override
    public void onError(final Throwable throwable) {
        subscriber.onError(throwable);
    }

    @Override
    public void request(final long n) {
        if (source == null) {
            return;
        }
        source.request(n);
    }

    @Override
    public void cancel() {
        if (source == null) {
            return;
        }
        source.cancel();
        source = null;
    }
}
