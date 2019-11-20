package ru.mail.polis.service.saloed.flow;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class OrderedMergeSubscription<T extends Comparable<T>> implements Subscription {

    private final Subscriber<? super T> subscriber;
    private final List<OrderedMergeSourceSubscriber> sources;
    private final List<SourceValue<T>> values;
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicLong requested = new AtomicLong();
    private final AtomicLong emitted = new AtomicLong();
    private final AtomicInteger publishRequests = new AtomicInteger();

    OrderedMergeSubscription(final Subscriber<? super T> subscriber, final int n) {
        this.subscriber = subscriber;
        this.sources = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            this.sources.add(new OrderedMergeSourceSubscriber());
        }
        this.values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(SourceValue.empty());
        }
    }

    void subscribe(final List<Publisher<T>> sources) {
        Streams.forEachPair(this.sources.stream(), sources.stream(),
            (sub, source) -> source.subscribe(sub));
    }

    @Override
    public void request(final long n) {
        requested.addAndGet(n);
        publish();
    }

    @Override
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            for (final var source : sources) {
                source.cancel();
            }
            if (publishRequests.getAndIncrement() == 0) {
                cleanup();
            }
        }
    }

    private void cleanup() {
        values.clear();
        for (final var source : sources) {
            source.cancel();
            source.clear();
        }
        sources.clear();
    }

    private void onSourceError(final Throwable ex) {
        error.set(ex);
        publish();
    }

    private void whenComplete() {
        final var ex = error.get();
        if (ex == null) {
            subscriber.onComplete();
        } else {
            subscriber.onError(ex);
        }
    }

    private long emit(final long emitted) {
        final long currentRequested = this.requested.get();
        long currentEmitted = emitted;
        while (true) {
            if (cancelled.get()) {
                return -1;
            }
            if (error.get() != null) {
                subscriber.onError(error.get());
                return -1;
            }
            final var valuesStats = actualizeValues();
            if (valuesStats.done == sources.size()) {
                whenComplete();
                return -1;
            }
            if (valuesStats.ready != sources.size() || currentEmitted >= currentRequested) {
                break;
            }
            final var minIndex = indexOfMinimal();
            final var min = values.get(minIndex).value;
            values.set(minIndex, SourceValue.empty());
            if (subscriber == null) {
                throw new IllegalStateException("Subscriber is not exists");
            }
            subscriber.onNext(min);

            currentEmitted++;
            sources.get(minIndex).request(1);
        }
        return currentEmitted;
    }

    private void publish() {
        if (publishRequests.getAndIncrement() != 0) {
            return;
        }
        int missedPublishRequests = 1;
        long currentEmitted = this.emitted.get();
        do {
            currentEmitted = emit(currentEmitted);
            if (currentEmitted == -1) {
                cleanup();
                return;
            }
            this.emitted.set(currentEmitted);
            missedPublishRequests = publishRequests.addAndGet(-missedPublishRequests);
        } while (missedPublishRequests != 0);
    }

    private SourceValueStats actualizeValues() {
        final var stats = new SourceValueStats();
        for (int i = 0; i < sources.size(); i++) {
            final var value = values.get(i);
            if (value.isDone()) {
                stats.done++;
                stats.ready++;
            } else if (value.isEmpty()) {
                final var source = sources.get(i);
                final var sourceExhausted = source.done.get();
                final var item = source.poll();
                if (item != null) {
                    values.set(i, SourceValue.of(item));
                    stats.ready++;
                    continue;
                }
                if (sourceExhausted) {
                    values.set(i, SourceValue.done());
                    stats.done++;
                    stats.ready++;
                }
            } else {
                stats.ready++;
            }
        }
        return stats;
    }

    private int indexOfMinimal() {
        T min = null;
        int minIndex = -1;
        for (int i = 0; i < values.size(); i++) {
            final var value = values.get(i);
            if (value.isDone() || value.isEmpty()) {
                continue;
            }
            final var smaller = min == null || min.compareTo(value.value) > 0;
            if (smaller) {
                min = value.value;
                minIndex = i;
            }
        }
        return minIndex;
    }

    private enum SourceValueType {
        VALUE, EMPTY, DONE
    }

    private static final class SourceValueStats {

        int done;
        int ready;
    }

    private static final class SourceValue<T> {

        static final SourceValue<?> EMPTY_VALUE = new SourceValue<>(SourceValueType.EMPTY, null);
        static final SourceValue<?> SOURCE_DONE = new SourceValue<>(SourceValueType.DONE, null);

        final SourceValueType type;
        final T value;

        private SourceValue(final SourceValueType type, final T value) {
            this.type = type;
            this.value = value;
        }

        static <T> SourceValue<T> empty() {
            @SuppressWarnings("unchecked") final SourceValue<T> empty = (SourceValue<T>) EMPTY_VALUE;
            return empty;
        }

        static <T> SourceValue<T> done() {
            @SuppressWarnings("unchecked") final SourceValue<T> done = (SourceValue<T>) SOURCE_DONE;
            return done;
        }

        static <T> SourceValue<T> of(final T value) {
            return new SourceValue<>(SourceValueType.VALUE, value);
        }

        boolean isEmpty() {
            return type == SourceValueType.EMPTY;
        }

        boolean isDone() {
            return type == SourceValueType.DONE;
        }
    }

    private final class OrderedMergeSourceSubscriber implements Subscriber<T>, Subscription {

        final AtomicBoolean done = new AtomicBoolean(false);
        private final Queue<T> queue = new ConcurrentLinkedQueue<>();
        private Subscription source;

        @Override
        public void onSubscribe(final Subscription subscription) {
            if (done.get()) {
                subscription.cancel();
                return;
            }
            source = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(final T item) {
            queue.offer(item);
            publish();
        }

        @Override
        public void onError(final Throwable throwable) {
            done.set(true);
            onSourceError(throwable);
        }

        @Override
        public void onComplete() {
            done.set(true);
            publish();
        }

        @Override
        public void request(final long n) {
            if (source == null) {
                return;
            }
            source.request(1);
        }

        @Override
        public void cancel() {
            done.set(true);
            if (source == null) {
                return;
            }
            source.cancel();
            source = null;
        }

        void clear() {
            queue.clear();
        }

        T poll() {
            return queue.poll();
        }

    }
}
