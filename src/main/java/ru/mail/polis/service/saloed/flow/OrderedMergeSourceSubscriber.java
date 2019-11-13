package ru.mail.polis.service.saloed.flow;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;

final class OrderedMergeSourceSubscriber<T extends Comparable<T>>
    implements Subscriber<T>, Subscription {

    final AtomicBoolean done = new AtomicBoolean(false);
    private final Queue<T> queue = new ConcurrentLinkedQueue<T>();
    private final OrderedMergeSubscription<T> merger;
    private Subscription source;

    OrderedMergeSourceSubscriber(final OrderedMergeSubscription<T> merger) {
        this.merger = merger;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        source = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T item) {
        queue.offer(item);
        merger.publish();
    }

    @Override
    public void onError(Throwable throwable) {
        done.set(true);
        merger.onSourceError(throwable);
    }

    @Override
    public void onComplete() {
        done.set(true);
        merger.publish();
    }

    @Override
    public void request(long n) {
        source.request(1);
    }

    @Override
    public void cancel() {
        source.cancel();
    }

    T poll() {
        return queue.poll();
    }

}
