package ru.mail.polis.service.saloed.flow;

import java.util.Iterator;
import java.util.concurrent.Flow;

public final class IteratorPublisher<T> extends SingleSubscriberPublisher<T> implements
    Flow.Subscription {

    private final Iterator<T> iterator;

    public IteratorPublisher(final Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    protected boolean isFinished() {
        return !iterator.hasNext();
    }

    @Override
    protected EmitStatus emit() {
        if (!iterator.hasNext()) {
            return EmitStatus.CANCEL;
        }
        final var next = iterator.next();
        subscriber.onNext(next);
        return EmitStatus.SUCCESS;
    }
}
