package ru.mail.polis.service.saloed.request.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.service.saloed.IOExceptionLight;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.ClusterNodeRouter;
import ru.mail.polis.service.saloed.StreamHttpSession;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordPayload;
import ru.mail.polis.service.saloed.payload.RecordWithTimestampAndKeyPayload;
import ru.mail.polis.service.saloed.request.RequestUtils;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public final class EntitiesRequestProcessor {
    private static final Log log = LogFactory.getLog(Stub.class);
    private final ClusterNodeRouter clusterNodeRouter;
    private final DAOWithTimestamp dao;
    public static final String REQUEST_PATH = "/v0/entities";

    public EntitiesRequestProcessor(final ClusterNodeRouter clusterNodeRouter,
                                    final DAOWithTimestamp dao) {
        this.clusterNodeRouter = clusterNodeRouter;
        this.dao = dao;
    }

    /**
     * Retrieve range for service.
     *
     * @param arguments     of request
     * @param streamSession session for response
     * @throws IOException if network error occurred
     */
    public void processForService(
            final Arguments arguments,
            final StreamHttpSession streamSession) throws IOException {
        performSingleNode(arguments, streamSession);
    }


    /**
     * Retrieve range for user. Result range is a merged ranges from all nodes.
     *
     * @param arguments     of request
     * @param streamSession session for response
     */
    public void processForUser(
            final Arguments arguments, final StreamHttpSession streamSession) {
        performNestedProcessing(arguments, streamSession);
    }

    private void performSingleNode(
            final Arguments arguments,
            final StreamHttpSession streamSession) throws IOException {
        final var iterator = dao.recordRange(arguments.start, arguments.end);
        final var payloadIterator = Iterators.transform(iterator,
                (record) -> (Payload) new RecordWithTimestampAndKeyPayload(record));
        streamSession.stream(payloadIterator);
    }

    private void performNestedProcessing(final Arguments arguments, final StreamHttpSession streamSession) {
        final var nodes = clusterNodeRouter.allNodes();
        final var iterators = new ArrayList<Iterator<RecordWithTimestampAndKey>>();
        final var futures = new ArrayList<CompletableFuture<Integer>>();
        for (final var node : nodes) {
            if (node.isLocal()) {
                final var iterator = dao.recordRange(arguments.start, arguments.end);
                iterators.add(iterator);
                continue;
            }
            final var client = node.getHttpClient();
            final Map<String, String> requestParams;
            if (arguments.hasEnd()) {
                requestParams = Map.of("start", arguments.startStr, "end", arguments.endStr);
            } else {
                requestParams = Map.of("start", arguments.startStr);
            }

            var requestBuilder = node.requestBuilder(REQUEST_PATH, requestParams);
            requestBuilder = RequestUtils.setRequestFromService(requestBuilder);
            final var request = requestBuilder.GET().build();
            final var subscriber = new Stub();
            final var future = client.sendAsync(request, HttpResponse.BodyHandlers.fromSubscriber(subscriber))
                    .thenApply(HttpResponse::statusCode);
            final var nonNullStub = Iterators.filter(subscriber, Objects::nonNull);
            final var recordIterator = Iterators
                    .transform(nonNullStub, RecordWithTimestampAndKey::fromRawBytes);
            iterators.add(recordIterator);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(__ -> {
                    if (futures.stream().map(CompletableFuture::join).allMatch(it -> it == 200)) {
                        final var mergedIterators = Iterators.mergeSorted(iterators, RecordWithTimestampAndKey::compareTo);
                        final var payloadIterator = new ReplicatedRecordsIterator(mergedIterators);
                        try {
                            streamSession.stream(payloadIterator);
                        } catch (IOException e) {
                            log.error("Error while streaming response");
                        }
                        return null;
                    }
                    try {
                        streamSession.sendError(Response.INTERNAL_ERROR, "");
                    } catch (IOException e) {
                        log.error("Error while send response");
                    }
                    return null;
                })
                .join();
    }

    // todo: replace with something normal
    private static class Stub implements Flow.Subscriber<List<ByteBuffer>>, Iterator<ByteBuffer> {


        private Flow.Subscription subscription;
        private boolean finished;
        private final BlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<>(1024);


        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            this.finished = false;
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> item) {
            for (final var element : item) {
                try {
                    queue.put(element);
                } catch (InterruptedException e) {
                    log.error("Stub put interrupted", e);
                }
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            log.error(throwable);
        }

        @Override
        public void onComplete() {
            finished = true;
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty() || !finished;
        }

        @Override
        public ByteBuffer next() {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                log.error("Error while retrieving element from Stub", e);
            }
            return null;
        }
    }

    private static class ReplicatedRecordsIterator implements Iterator<Payload> {

        private final PeekingIterator<RecordWithTimestampAndKey> iterator;
        private RecordWithTimestampAndKey nextRecord;

        ReplicatedRecordsIterator(final Iterator<RecordWithTimestampAndKey> iterator) {
            this.iterator = Iterators.peekingIterator(iterator);
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        private void advance() {
            do {
                nextRecord = advanceRecord();
            }
            while (nextRecord != null && nextRecord.isEmpty());
        }

        private RecordWithTimestampAndKey advanceRecord() {
            RecordWithTimestampAndKey record = null;
            while (iterator.hasNext()) {
                record = iterator.next();
                if (!iterator.hasNext()) {
                    break;
                }
                final var next = iterator.peek();
                if (!record.sameKeyRecords(next)) {
                    break;
                }
            }
            return record;
        }

        @Override
        public Payload next() {
            final var recordWithTimestampAndKey = nextRecord;
            advance();
            final var record = Record
                    .of(recordWithTimestampAndKey.getKey(), recordWithTimestampAndKey.getValue());
            return new RecordPayload(record);
        }
    }

    public static class Arguments {
        private final String startStr;
        private final String endStr;
        private final ByteBuffer start;
        private final ByteBuffer end;
        private final boolean isServiceRequest;

        private Arguments(
                final String startStr,
                final String endStr,
                final ByteBuffer start,
                final ByteBuffer end,
                final boolean isServiceRequest) {
            this.startStr = startStr;
            this.endStr = endStr;
            this.start = start;
            this.end = end;
            this.isServiceRequest = isServiceRequest;
        }

        public boolean isServiceRequest() {
            return isServiceRequest;
        }

        public boolean hasEnd() {
            return endStr != null && end != null;
        }

        public static Arguments parse(final String start, final String end, final boolean isServiceRequest) {
            if (start == null || start.isEmpty()) {
                return null;
            }
            final var startBytes = ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));
            ByteBuffer endBytes = null;
            if (end != null && !end.isEmpty()) {
                endBytes = ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8));
            }
            return new Arguments(start, end, startBytes, endBytes, isServiceRequest);
        }
    }
}
