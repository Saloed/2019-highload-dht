package ru.mail.polis.service.saloed.request.processor;

import com.google.common.collect.Iterators;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscribers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.ClusterNodeRouter;
import ru.mail.polis.service.saloed.IOExceptionLight;
import ru.mail.polis.service.saloed.flow.IteratorPublisher;
import ru.mail.polis.service.saloed.flow.OrderedMergeProcessor;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordWithTimestampAndKeyPayload;
import ru.mail.polis.service.saloed.request.RequestUtils;
import ru.mail.polis.service.saloed.request.processor.entities.Arguments;
import ru.mail.polis.service.saloed.request.processor.entities.RecordsFromBytesProcessor;
import ru.mail.polis.service.saloed.request.processor.entities.ReplicatedRecordsProcessor;

public final class EntitiesRequestProcessor {

    public static final String REQUEST_PATH = "/v0/entities";
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
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
        final var publisher = new IteratorPublisher<>(payloadIterator);
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
        final Map<String, String> requestParams;
        if (arguments.hasEnd()) {
            requestParams = Map.of(
                "start", arguments.getStartStr(),
                "end", arguments.getEndStr());
        } else {
            requestParams = Map.of("start", arguments.getStartStr());
        }
        final var publishers = new ArrayList<Publisher<RecordWithTimestampAndKey>>(nodes.size());
        for (final var node : nodes) {
            if (node.isLocal()) {
                continue;
            }
            final var client = node.getHttpClient();
            var requestBuilder = node
                .requestBuilder(REQUEST_PATH, requestParams)
                .timeout(TIMEOUT);
            requestBuilder = RequestUtils.setRequestFromService(requestBuilder);
            final var request = requestBuilder.GET().build();
            final var recordProcessor = new RecordsFromBytesProcessor();
            client
                .sendAsync(request, responseInfo -> {
                    if (responseInfo.statusCode() != 200) {
                        recordProcessor.onError(new IOExceptionLight("Response status is not OK"));
                        return BodySubscribers.discarding();
                    }
                    return BodySubscribers.fromSubscriber(recordProcessor);
                })
                .thenApply(HttpResponse::body)
                .exceptionally(ex -> {
                    recordProcessor.onError(ex);
                    return null;
                });
            publishers.add(recordProcessor);
        }
        final var iterator = dao.recordRange(arguments.getStart(), arguments.getEnd());
        final var localPublisher = new IteratorPublisher<>(iterator);
        publishers.add(localPublisher);
        final var mergedPublisher = OrderedMergeProcessor.merge(publishers);
        final var resultPublisher = new ReplicatedRecordsProcessor();
        mergedPublisher.subscribe(resultPublisher);
        resultPublisher.subscribe(subscriber);
    }

}
