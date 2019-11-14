package ru.mail.polis.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.google.common.primitives.Bytes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.RepeatedTest;
import ru.mail.polis.Record;

class ShardingEntriesTest extends ShardingTest {

    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    @RepeatedTest(10)
    void allEntriesExists() {
        checkCorrectness(10000, 1, new AllChunksPartitioner());
    }

    @RepeatedTest(10)
    void rangeEntriesCorrect() {
        checkCorrectness(10000, 1, new ChunkPartitioner());
    }

    @RepeatedTest(10)
    void allReplicatedEntriesExists() {
        checkCorrectness(10000, 2, new AllChunksPartitioner());
    }

    @RepeatedTest(10)
    void rangeReplicatedEntriesCorrect() {
        checkCorrectness(10000, 2, new ChunkPartitioner());
    }


    private List<Chunk> fullFillCluster(final int entriesCount, final int replicas) {
        final var chunks = new HashSet<Chunk>();
        for (int i = 0; i < entriesCount; i++) {
            final var key = randomId();
            final var value = randomValue();
            final var chunk = new Chunk(key, value);
            chunks.add(chunk);
            final var nodeId = ThreadLocalRandom.current().nextInt(0, endpoints.size() - 1);
            assertTimeoutPreemptively(TIMEOUT, () -> {
                assertEquals(201, upsert(nodeId, key, value, replicas, replicas).getStatus());
            });
        }
        return new ArrayList<>(chunks);
    }

    private byte[] joinChunksBytes(final List<Chunk> chunks) {
        final var chunksBytes = chunks.stream().map(Chunk::getBytes).collect(Collectors.toList());
        final var arraySize = chunksBytes.stream().map(it -> it.length).reduce(0, Integer::sum);
        final var resultBuffer = ByteBuffer.allocate(arraySize);
        for (final var chunkBytes : chunksBytes) {
            resultBuffer.put(chunkBytes);
        }
        return resultBuffer.array();
    }

    private void checkCorrectness(
        final int totalRecords,
        final int replicationFactor,
        final ChunkPartitioner partitioner) {
        final var chunks = fullFillCluster(totalRecords, replicationFactor);
        chunks.sort(Chunk::compareTo);
        final var partition = partitioner.fromChunks(chunks);
        final var expectedBytes = joinChunksBytes(partition.expectedChunks);
        for (int i = 0; i < endpoints.size(); i++) {
            assertTimeoutPreemptively(TIMEOUT, () -> {
                final Response response = range(1, partition.rangeStart, partition.rangeEnd);
                assertEquals(200, response.getStatus());
                final var body = response.getBody();
                assertArrayEquals(expectedBytes, body);
            });
        }
    }

    private static class ChunkPartitioner {

        ChunkPartition fromChunks(final List<Chunk> chunks) {
            final var splitIdx = ThreadLocalRandom.current().nextInt(1, chunks.size() / 4);
            final var rangeFrom = chunks.get(splitIdx);
            final var rangeTo = chunks.get(chunks.size() - splitIdx);
            final var expectedChunks = chunks.subList(splitIdx, chunks.size() - splitIdx);
            return new ChunkPartition(rangeFrom.key, rangeTo.key, chunks, expectedChunks);
        }

    }

    private static class AllChunksPartitioner extends ChunkPartitioner {

        @Override
        ChunkPartition fromChunks(final List<Chunk> chunks) {
            return new ChunkPartition(chunks.get(0).key, null, chunks, chunks);
        }
    }

    private static class ChunkPartition {

        final String rangeStart;
        final String rangeEnd;
        final List<Chunk> chunks;
        final List<Chunk> expectedChunks;

        ChunkPartition(
            final String rangeStart,
            final String rangeEnd,
            final List<Chunk> chunks,
            final List<Chunk> expectedChunks) {
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.chunks = chunks;
            this.expectedChunks = expectedChunks;
        }
    }


    private static class Chunk implements Comparable<Chunk> {

        private final String key;
        private final byte[] value;
        private final Record record;
        private final byte[] bytes;

        Chunk(final String key, final byte[] value) {
            this.key = key;
            this.value = value;
            final var keyBytes = key.getBytes(StandardCharsets.UTF_8);
            this.record = Record.of(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(value));
            final var delimiter = "\n".getBytes(StandardCharsets.UTF_8);
            this.bytes = Bytes.concat(keyBytes, delimiter, value);
        }

        @Override
        public int compareTo(@NotNull Chunk chunk) {
            return record.compareTo(chunk.record);
        }

        public byte[] getBytes() {
            return bytes;
        }

        public String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Chunk)) {
                return false;
            }
            Chunk chunk = (Chunk) o;
            return key.equals(chunk.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

    }

}
