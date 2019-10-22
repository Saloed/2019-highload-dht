package ru.mail.polis.service.saloed;

import com.google.common.collect.Iterators;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

public class EntitiesRequestProcessor {

    private final Topology topology;
    private final DAOWithTimestamp dao;
    private final Map<String, StreamHttpClient> pool;

    EntitiesRequestProcessor(final Topology topology, final DAOWithTimestamp dao,
        final Map<String, StreamHttpClient> pool) {
        this.topology = topology;
        this.dao = dao;
        this.pool = pool;
    }

    public void processForService(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final RecordStreamHttpSession streamSession) throws IOException {
        performSingleNode(start, end, streamSession);
    }

    public void processForUser(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final Request request,
        final RecordStreamHttpSession streamSession) throws IOException {
        final var arguments = new ProcessorArguments(start, end, request, streamSession);
        final var nodes = topology.allNodes().iterator();
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
        if (topology.isMe(node)) {
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
                    final var str = new String(bytes, StandardCharsets.UTF_8);
                    final var firstDelimiter = str.indexOf('\n');
                    final var keyStr = str.substring(0, firstDelimiter);
                    final var valueStr = str.substring(firstDelimiter);
                    final var keyBytes = ByteBuffer
                        .wrap(keyStr.getBytes(StandardCharsets.UTF_8));
                    final var valueBytes = ByteBuffer
                        .wrap(valueStr.getBytes(StandardCharsets.UTF_8));
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

        public ProcessorArguments(
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
