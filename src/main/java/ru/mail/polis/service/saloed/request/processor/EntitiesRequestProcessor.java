package ru.mail.polis.service.saloed.request.processor;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ru.mail.polis.Record;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.ClusterNodeRouter;
import ru.mail.polis.service.saloed.IOExceptionLight;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordPayload;
import ru.mail.polis.service.saloed.payload.RecordWithTimestampAndKeyPayload;
import ru.mail.polis.service.saloed.request.RequestUtils;

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
     * @param arguments of request
     * @return iterator over data
     */
    public CompletableFuture<Iterator<Payload>> processForService(
        final Arguments arguments) {
        final var iterator = dao.recordRange(arguments.start, arguments.end);
        final var payloadIterator = Iterators.transform(iterator,
            (record) -> (Payload) new RecordWithTimestampAndKeyPayload(record));
        return CompletableFuture.completedFuture(payloadIterator);
    }


    /**
     * Retrieve range for user. Result range is a merged ranges from all nodes.
     *
     * @param arguments of request
     * @return iterator over data
     */
    public CompletableFuture<Iterator<Payload>> processForUser(final Arguments arguments) {
        final var nodes = clusterNodeRouter.allNodes();
        final var iterators = new ArrayList<Iterator<RecordWithTimestampAndKey>>();
        final var futures = new ArrayList<CompletableFuture<Iterator<RecordWithTimestampAndKey>>>();
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
            final var future = client
                .sendAsync(request, new Stub())
                .thenApply(HttpResponse::body);
            futures.add(future);
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(__ -> {
                final var futureResults = futures.stream().map(CompletableFuture::join);
                final var allIterators = Stream.concat(futureResults, iterators.stream())
                    .collect(Collectors.toList());
                final var mergedIterators = Iterators
                    .mergeSorted(allIterators, RecordWithTimestampAndKey::compareTo);
                return new ReplicatedRecordsIterator(mergedIterators);
            });
    }

    /**
     * Iterator over response body. todo: replace with something normal.
     */
    private static class Stub implements
        BodyHandler<Iterator<RecordWithTimestampAndKey>>,
        BodySubscriber<Iterator<RecordWithTimestampAndKey>>,
        Flow.Subscriber<List<ByteBuffer>>,
        Iterator<CompletableFuture<Optional<RecordWithTimestampAndKey>>> {

        private static final int FETCH_SIZE = 128;

        private final AtomicBoolean finished = new AtomicBoolean(false);
        private ResponseInfo responseInfo;
        private ByteBuffer lastReceived = ByteBuffer.allocate(0);
        private Queue<RecordWithTimestampAndKey> records = new ConcurrentLinkedDeque<>();
        private Queue<CompletableFuture<Optional<RecordWithTimestampAndKey>>> nextOffers = new ConcurrentLinkedDeque<>();
        private CompletableFuture<Void> initialized = new CompletableFuture<>();
        private Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            initialized.complete(null);
        }

        private ByteBuffer extend(final ByteBuffer buffer, final ByteBuffer other) {
            return ByteBuffer.allocate(buffer.remaining() + other.remaining())
                .put(buffer)
                .put(other)
                .rewind();
        }

        private ByteBuffer truncate(final ByteBuffer buffer) {
            if (buffer.remaining() == 0) {
                return ByteBuffer.allocate(0);
            }
            final var array = ByteBufferUtils.toArray(buffer);
            return ByteBuffer.wrap(array);
        }

        @Override
        public void onNext(List<ByteBuffer> item) {
            for (final var buffer : item) {
                lastReceived = extend(lastReceived, buffer);
                while (RecordWithTimestampAndKey.mayDeserialize(lastReceived)) {
                    final var record = RecordWithTimestampAndKey.fromRawBytes(lastReceived);
                    records.add(record);
                }
                lastReceived = truncate(lastReceived);
            }
            if (records.isEmpty()) {
                subscription.request(FETCH_SIZE);
                return;
            }
            while (!nextOffers.isEmpty() && !records.isEmpty()) {
                final var nextRecord = nextOffers.poll();
                final var next = records.poll();
                nextRecord.complete(Optional.of(next));
            }
            if (records.isEmpty()) {
                subscription.request(FETCH_SIZE);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            nextOffers.iterator().forEachRemaining(it -> it.completeExceptionally(throwable));
            nextOffers.clear();
            finished.set(true);
            initialized.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            finished.set(true);
            if (records.isEmpty()) {
                nextOffers.iterator().forEachRemaining(it -> it.complete(Optional.empty()));
                nextOffers.clear();
            }
        }

        @Override
        public boolean hasNext() {
            return !(finished.get() && records.isEmpty());
        }

        @Override
        public CompletableFuture<Optional<RecordWithTimestampAndKey>> next() {
            final var next = records.poll();
            if (next != null) {
                return CompletableFuture.completedFuture(Optional.of(next));
            }
            final var nextRecord = new CompletableFuture<Optional<RecordWithTimestampAndKey>>();
            nextOffers.add(nextRecord);
            subscription.request(FETCH_SIZE);
            return nextRecord;
        }

        @Override
        public CompletionStage<Iterator<RecordWithTimestampAndKey>> getBody() {
            if (responseInfo == null || responseInfo.statusCode() != 200) {
                return CompletableFuture
                    .failedFuture(new IOExceptionLight("Response status is not OK"));
            }
            return initialized.thenApply(__ -> {
                final Iterator<Optional<RecordWithTimestampAndKey>> awaitIterator = Iterators
                    .transform(this, future -> {
                        if (future == null) {
                            return Optional.empty();
                        }
                        try {
                            return future.get(100, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            return Optional.empty();
                        }
                    });
                final var nonEmptyIterator = Iterators.filter(awaitIterator, Optional::isPresent);
                return Iterators.transform(nonEmptyIterator, Optional::get);
            });
        }

        @Override
        public BodySubscriber<Iterator<RecordWithTimestampAndKey>> apply(
            final ResponseInfo responseInfo) {
            this.responseInfo = responseInfo;
            return this;
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

        public static Arguments parse(final String start, final String end,
            final boolean isServiceRequest) {
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

        public boolean isServiceRequest() {
            return isServiceRequest;
        }

        public boolean hasEnd() {
            return endStr != null && end != null;
        }
    }
}
