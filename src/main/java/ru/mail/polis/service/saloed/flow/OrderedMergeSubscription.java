package ru.mail.polis.service.saloed.flow;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class OrderedMergeSubscription<T extends Comparable<T>> implements Subscription {

    private final Subscriber<? super T> subscriber;
    private final List<OrderedMergeSourceSubscriber<T>> sources;
    private final List<SourceValue<T>> values;
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicLong requested = new AtomicLong();
    private final AtomicLong emitted = new AtomicLong();
    private final AtomicInteger wip = new AtomicInteger();


    OrderedMergeSubscription(final Subscriber<? super T> subscriber, final int n) {
        this.subscriber = subscriber;
        this.sources = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final var source = new OrderedMergeSourceSubscriber<>(this);
            this.sources.add(source);
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
            wip.getAndIncrement();
        }
    }

    void onSourceError(final Throwable ex) {
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
            subscriber.onNext(min);

            currentEmitted++;
            sources.get(minIndex).request(1);
        }
        return currentEmitted;
    }

    void publish() {
        if (wip.getAndIncrement() != 0) {
            return;
        }
        int missed = 1;
        long currentEmitted = this.emitted.get();
        do {
            currentEmitted = emit(currentEmitted);
            if (currentEmitted == -1) {
                return;
            }
            this.emitted.set(currentEmitted);
            missed = wip.addAndGet(-missed);
        } while (missed != 0);
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

}
