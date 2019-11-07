package ru.mail.polis.service.saloed;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

final class CompletableFutureUtils {

    private CompletableFutureUtils() {
    }

    /**
     * Creates a new CompletableFuture, which completes after specified amount of input futures
     * completes, and has values in their options, or whenever it is impossible to achieve required
     * amount. If any of input futures completes with exception -> result future also completes with
     * the same exception.
     *
     * @param futures to combine
     * @param amount  required to complete
     * @param <T>     type of result
     * @return future
     */
    static <T> CompletableFuture<Collection<T>> someOf(
        final List<CompletableFuture<Optional<T>>> futures,
        final int amount) {
        final var result = new CompletableFuture<Collection<T>>();
        final var resultValue = new ConcurrentLinkedQueue<T>();
        final var errorCounter = new AtomicInteger(futures.size() - amount);
        for (final var future : futures) {
            future
                .thenAccept(value -> {
                    value.ifPresentOrElse(resultValue::add, errorCounter::decrementAndGet);
                    if (resultValue.size() >= amount || errorCounter.get() < 0) {
                        result.complete(resultValue);
                    }
                })
                .exceptionally(ex -> {
                    result.completeExceptionally(ex);
                    return null;
                });
        }
        return result;
    }
}
