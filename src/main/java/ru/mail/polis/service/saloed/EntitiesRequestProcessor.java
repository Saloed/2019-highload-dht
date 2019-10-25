package ru.mail.polis.service.saloed;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.IOExceptionLight;

final class EntitiesRequestProcessor {

    private final ClusterNodeRouter clusterNodeRouter;
    private final DAOWithTimestamp dao;
    private final Map<String, StreamHttpClient> pool;

    EntitiesRequestProcessor(final ClusterNodeRouter clusterNodeRouter, final DAOWithTimestamp dao,
        final Map<String, StreamHttpClient> pool) {
        this.clusterNodeRouter = clusterNodeRouter;
        this.dao = dao;
        this.pool = pool;
    }

    void processForService(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final RecordStreamHttpSession streamSession) throws IOException {
        performSingleNode(start, end, streamSession);
    }

    void processForUser(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final Request request,
        final RecordStreamHttpSession streamSession) throws IOException {
        final var arguments = new ProcessorArguments(start, end, request, streamSession);
        final var nodes = clusterNodeRouter.allNodes().iterator();
        final var iterators = new ArrayList<Iterator<Record>>();
        try {
            performNestedProcessing(arguments, nodes, iterators);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOExceptionLight("Error in nested processor", e);
        }
    }

    private void performSingleNode(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final RecordStreamHttpSession streamSession) throws IOException {
        final var iterator = dao.range(start, end);
        streamSession.stream(iterator);
    }

    private void performNestedProcessing(
        final ProcessorArguments arguments,
        final Iterator<String> nodes,
        final List<Iterator<Record>> iterators)
        throws IOException, InterruptedException, HttpException, PoolException {
        if (!nodes.hasNext()) {
            final var mergedIterators = Iterators.mergeSorted(iterators, Record::compareTo);
            arguments.streamSession.stream(mergedIterators);
            return;
        }
        final var node = nodes.next();
        if (clusterNodeRouter.isMe(node)) {
            final var iterator = dao.range(arguments.start, arguments.end);
            iterators.add(iterator);
            performNestedProcessing(arguments, nodes, iterators);
        } else {
            final var client = pool.get(node);
            client.invokeStream(arguments.request, iterator -> {
                if (iterator.getResponse().getStatus() != 200 || iterator.isNotAvailable()) {
                    throw new IOExceptionLight("Unexpected response from node");
                }
                final var recordIterator = Iterators.transform(iterator, (bytes) -> {
                    final var delimiter = "\n".getBytes(StandardCharsets.UTF_8)[0];
                    final var delimiterIdx = Bytes.indexOf(bytes, delimiter);
                    final var keyByteArray = Arrays.copyOfRange(bytes, 0, delimiterIdx);
                    final var valueByteArray = Arrays.copyOfRange(bytes, delimiterIdx + 1, bytes.length);
                    final var keyBytes = ByteBuffer.wrap(keyByteArray);
                    final var valueBytes = ByteBuffer .wrap(valueByteArray);
                    return Record.of(keyBytes, valueBytes);
                });
                iterators.add(recordIterator);
                performNestedProcessing(arguments, nodes, iterators);
            });
        }
    }

    private static class ProcessorArguments {

        final ByteBuffer start;
        final ByteBuffer end;
        final Request request;
        final RecordStreamHttpSession streamSession;

        ProcessorArguments(
            final ByteBuffer start,
            final ByteBuffer end,
            final Request request,
            final RecordStreamHttpSession streamSession) {

            this.start = start;
            this.end = end;
            this.request = request;
            this.streamSession = streamSession;
        }
    }

}
