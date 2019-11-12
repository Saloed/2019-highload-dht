package ru.mail.polis.service.saloed.request.processor.entities;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

public abstract class SingleSubscriberPublisher<T> implements Flow.Publisher<T> {

    protected Subscriber<? super T> subscriber = null;

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        if (this.subscriber != null) {
            throw new IllegalStateException("Single subscriber publisher already has a subscriber");
        }
        this.subscriber = subscriber;
        this.subscriber.onSubscribe(getSubscription());
    }

    abstract Subscription getSubscription();
}
