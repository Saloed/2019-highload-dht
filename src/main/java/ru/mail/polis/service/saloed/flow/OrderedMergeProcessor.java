package ru.mail.polis.service.saloed.flow;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;

public final class OrderedMergeProcessor<T extends Comparable<T>> implements Publisher<T> {

    private final List<Publisher<T>> sources;

    private OrderedMergeProcessor(final List<Publisher<T>> sources) {
        this.sources = sources;
    }

    /**
     * Merge given publishers in ascending order according to comparator.
     *
     * @param sources publishers
     * @param <T>     comparable type
     * @return single publisher
     */
    public static <T extends Comparable<T>> Publisher<T> merge(final List<Publisher<T>> sources) {
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("Nothing to merge");
        }
        if (sources.size() == 1) {
            return sources.get(0);
        }
        return new OrderedMergeProcessor<T>(sources);
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        final var merger = new OrderedMergeSubscription<T>(subscriber, sources.size());
        subscriber.onSubscribe(merger);
        merger.subscribe(sources);
    }

}
