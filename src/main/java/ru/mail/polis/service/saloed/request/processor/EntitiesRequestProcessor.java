package ru.mail.polis.service.saloed.request.processor;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.IOExceptionLight;
import ru.mail.polis.dao.RecordWithTimestampAndKey;
import ru.mail.polis.service.saloed.ClusterNodeRouter;
import ru.mail.polis.service.saloed.StreamHttpSession;
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.payload.RecordPayload;
import ru.mail.polis.service.saloed.payload.RecordWithTimestampAndKeyPayload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class EntitiesRequestProcessor {

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
     * @param start         of range
     * @param end           of range (optional)
     * @param streamSession session for response
     * @throws IOException if network error occurred
     */
    public void processForService(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final StreamHttpSession streamSession) throws IOException {
        performSingleNode(start, end, streamSession);
    }


    /**
     * Retrieve range for user. Result range is a merged ranges from all nodes.
     *
     * @param start         of range
     * @param end           of range (optional)
     * @param request       original HTTP request
     * @param streamSession session for response
     * @throws IOException if network error occurred
     */
    public void processForUser(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final Request request,
        final StreamHttpSession streamSession) throws IOException {
        final var arguments = new ProcessorArguments(start, end, request, streamSession);
        final var nodes = clusterNodeRouter.allNodes().iterator();
        final var iterators = new ArrayList<Iterator<RecordWithTimestampAndKey>>();
        try {
            performNestedProcessing(arguments, nodes, iterators);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOExceptionLight("Error in nested processor", e);
        }
    }

    private void performSingleNode(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final StreamHttpSession streamSession) throws IOException {
        final var iterator = dao.recordRange(start, end);
        final var payloadIterator = Iterators.transform(iterator,
            (record) -> (Payload) new RecordWithTimestampAndKeyPayload(record));
        streamSession.stream(payloadIterator);
    }

    private void performNestedProcessing(
        final ProcessorArguments arguments,
        final Iterator<ClusterNodeRouter.ClusterNode> nodes,
        final List<Iterator<RecordWithTimestampAndKey>> iterators)
        throws IOException, InterruptedException, HttpException, PoolException {
        if (!nodes.hasNext()) {
            final var mergedIterators = Iterators
                .mergeSorted(iterators, RecordWithTimestampAndKey::compareTo);
            final var payloadIterator = new ReplicatedRecordsIterator(mergedIterators);
            arguments.streamSession.stream(payloadIterator);
            return;
        }
        final var node = nodes.next();
        if (node.isLocal()) {
            final var iterator = dao.recordRange(arguments.start, arguments.end);
            iterators.add(iterator);
            performNestedProcessing(arguments, nodes, iterators);
        } else {
            final var client = node.getHttpClient();
            client.invokeStream(arguments.request, iterator -> {
                if (iterator.getResponse().getStatus() != 200 || iterator.isNotAvailable()) {
                    throw new IOExceptionLight("Unexpected response from node");
                }
                final var recordIterator = Iterators
                    .transform(iterator, RecordWithTimestampAndKey::fromRawBytes);
                iterators.add(recordIterator);
                performNestedProcessing(arguments, nodes, iterators);
            });
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
            while (iterator.hasNext()) {
                final var current = iterator.next();
                if (!iterator.hasNext()) {
                    return current;
                }
                final var next = iterator.peek();
                if (current.sameKeyRecords(next)) {
                    continue;
                }
                return current;
            }
            return null;
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

    private static class ProcessorArguments {

        final ByteBuffer start;
        final ByteBuffer end;
        final Request request;
        final StreamHttpSession streamSession;

        ProcessorArguments(
            final ByteBuffer start,
            final ByteBuffer end,
            final Request request,
            final StreamHttpSession streamSession) {

            this.start = start;
            this.end = end;
            this.request = request;
            this.streamSession = streamSession;
        }
    }

}
