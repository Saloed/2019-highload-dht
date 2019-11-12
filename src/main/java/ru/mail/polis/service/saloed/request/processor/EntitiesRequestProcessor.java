package ru.mail.polis.service.saloed.request.processor;

import com.google.common.collect.Iterators;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.ClusterNodeRouter;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordWithTimestampAndKeyPayload;
import ru.mail.polis.service.saloed.request.RequestUtils;
import ru.mail.polis.service.saloed.request.processor.entities.Arguments;
import ru.mail.polis.service.saloed.request.processor.entities.BodyHandlerStub;
import ru.mail.polis.service.saloed.request.processor.entities.PayloadIteratorPublisher;
import ru.mail.polis.service.saloed.request.processor.entities.ReplicatedRecordsIterator;

public final class EntitiesRequestProcessor {

    public static final String REQUEST_PATH = "/v0/entities";
    private final ClusterNodeRouter clusterNodeRouter;
    private final DAOWithTimestamp dao;

    public EntitiesRequestProcessor(final ClusterNodeRouter clusterNodeRouter,
        final DAOWithTimestamp dao) {
        this.clusterNodeRouter = clusterNodeRouter;
        this.dao = dao;
    }

    /**
     * Retrieve range for service.
     *
     * @param arguments  of request
     * @param subscriber to the range
     */
    public void processForService(
        final Arguments arguments, final Subscriber<Payload> subscriber) {
        final var iterator = dao.recordRange(arguments.getStart(), arguments.getEnd());
        final var payloadIterator = Iterators.transform(iterator,
            (record) -> (Payload) new RecordWithTimestampAndKeyPayload(record));
        final var publisher = new PayloadIteratorPublisher(payloadIterator);
        publisher.subscribe(subscriber);
    }

    /**
     * Retrieve range for user. Result range is a merged ranges from all nodes.
     *
     * @param arguments  of request
     * @param subscriber to the range
     */
    public void processForUser(final Arguments arguments,
        final Subscriber<Payload> subscriber) {
        final var nodes = clusterNodeRouter.allNodes();
        final var iterators = new ArrayList<Iterator<RecordWithTimestampAndKey>>();
        final var futures = new ArrayList<CompletableFuture<Iterator<RecordWithTimestampAndKey>>>();
        for (final var node : nodes) {
            if (node.isLocal()) {
                final var iterator = dao.recordRange(arguments.getStart(), arguments.getEnd());
                iterators.add(iterator);
                continue;
            }
            final var client = node.getHttpClient();
            final Map<String, String> requestParams;
            if (arguments.hasEnd()) {
                requestParams = Map.of(
                    "start", arguments.getStartStr(),
                    "end", arguments.getEndStr());
            } else {
                requestParams = Map.of("start", arguments.getStartStr());
            }
            var requestBuilder = node.requestBuilder(REQUEST_PATH, requestParams);
            requestBuilder = RequestUtils.setRequestFromService(requestBuilder);
            final var request = requestBuilder.GET().build();
            final var future = client
                .sendAsync(request, new BodyHandlerStub())
                .thenApply(HttpResponse::body);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(__ -> {
                final var futureResults = futures.stream().map(CompletableFuture::join);
                final var allIterators = Stream.concat(futureResults, iterators.stream())
                    .collect(Collectors.toList());
                final var mergedIterators = Iterators
                    .mergeSorted(allIterators, RecordWithTimestampAndKey::compareTo);
                final var iterator = new ReplicatedRecordsIterator(mergedIterators);
                final var publisher = new PayloadIteratorPublisher(iterator);
                publisher.subscribe(subscriber);
                return null;
            }).exceptionally(ex -> {
            throw new RuntimeException("Iterator", ex);
        });
    }

}
