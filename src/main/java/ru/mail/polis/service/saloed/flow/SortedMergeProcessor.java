package ru.mail.polis.service.saloed.flow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;

public class SortedMergeProcessor<T extends Comparable<T>>
    extends SingleSubscriberPublisher<T> {

    private final Comparator<SourceSubscriber<T>> comparator;
    private final List<SourceSubscriber<T>> sources;


    private SortedMergeProcessor(final List<SourceSubscriber<T>> sources) {
        this.sources = sources;
        comparator = Comparator.comparing(SourceSubscriber::peek);
    }

    public static <T extends Comparable<T>> SortedMergeProcessor<T> subscribeOn(
        final List<Publisher<T>> publishers) {
        final var sources = new ArrayList<SourceSubscriber<T>>(publishers.size());
        final var processor = new SortedMergeProcessor<>(sources);
        for (final var publisher : publishers) {
            final var source = new SourceSubscriber<T>(processor);
            publisher.subscribe(source);
            sources.add(source);
        }
        return processor;
    }

    private void sourceError(final Throwable throwable) {
        error = throwable;
        sources.forEach(SourceSubscriber::cancel);
        publish();
    }

    @Override
    protected EmitStatus emit() {
        if (sources.stream().anyMatch(SourceSubscriber::isNotReady)) {
            return EmitStatus.CANCEL;
        }
        final var source = sources.stream().min(comparator);
        if (source.isEmpty()) {
            return EmitStatus.TRY_AGAIN;
        }
        final var src = source.get();
        final var next = src.poll();
        src.request(1);
        if (next == null) {
            return EmitStatus.TRY_AGAIN;
        }
        subscriber.onNext(next);
        return EmitStatus.SUCCESS;
    }

    @Override
    protected boolean isFinished() {
        return sources.stream().allMatch(SourceSubscriber::isExhausted);
    }

    private static final class SourceSubscriber<T extends Comparable<T>> implements Subscriber<T> {

        private final Queue<T> records = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean completed = new AtomicBoolean(false);
        private final SortedMergeProcessor<T> processor;
        private Subscription subscription;

        SourceSubscriber(final SortedMergeProcessor<T> processor) {
            this.processor = processor;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }


        @Override
        public void onNext(T item) {
            records.add(item);
            processor.publish();
        }

        @Override
        public void onError(Throwable throwable) {
            completed.set(true);
            processor.sourceError(throwable);
        }

        @Override
        public void onComplete() {
            completed.set(true);
            processor.publish();
        }

        void request(final long n) {
            if (completed.get()) {
                return;
            }
            subscription.request(n);
        }

        void cancel() {
            if (completed.get()) {
                return;
            }
            subscription.cancel();
        }

        T peek() {
            return records.element();
        }

        T poll() {
            return records.poll();
        }

        boolean isNotReady() {
            return records.isEmpty() && !completed.get();
        }

        boolean isExhausted() {
            return completed.get() && records.isEmpty();
        }

    }
}
