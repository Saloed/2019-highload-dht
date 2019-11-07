package ru.mail.polis.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import ru.mail.polis.TestBase;
import ru.mail.polis.service.saloed.topology.ConsistentHashTopology;
import ru.mail.polis.service.saloed.topology.Topology;

class HashConsistencyTest extends TestBase {

    private Topology<Integer> createTopology(final int nodesAmount) {
        final var nodeList = IntStream.range(0, nodesAmount).boxed().collect(Collectors.toList());
        return ConsistentHashTopology.forNodes(nodeList);
    }

    @Test
    void testSameNodes() {
        final var topologies = IntStream.rangeClosed(0, 10)
                .mapToObj(__ -> createTopology(10))
                .collect(Collectors.toList());
        for (int i = 0; i < 100000; i++) {
            final var key = randomId().getBytes(StandardCharsets.UTF_8);
            final var uniqueNodesCount = topologies.stream()
                    .map(topology -> topology.selectNode(ByteBuffer.wrap(key), 3))
                    .distinct()
                    .count();
            assertEquals(1, uniqueNodesCount, "All nodes must be equals");
        }
    }

    @Test
    void testDistributionUniformness() {
        final var topology = createTopology(100);
        testDistributionUniformness(topology, 100_000, 17, 0.1);
    }

    @Test
    void testDistributionUniformnessAfterNodeAddition() {
        final var startNodesAmount = 20;
        final var endNodesAmount = startNodesAmount * 3;
        var topology = createTopology(startNodesAmount);
        testDistributionUniformness(topology, 100_000, 3, 0.1);
        var nodeId = topology.nodesAmount() + 10;
        while (topology.nodesAmount() < endNodesAmount) {
            topology = topology.addNode(nodeId++);
            testDistributionUniformness(topology, 100_000, 3, 0.1);
        }
    }

    private void testDistributionUniformness(
            final Topology<Integer> topology,
            final int iterationsCount,
            final int nodesPerSample,
            final double possibleDeviation
    ) {
        final var nodesCounters = new HashMap<Integer, Integer>();
        for (int i = 0; i < iterationsCount; i++) {
            final var key = randomId().getBytes(StandardCharsets.UTF_8);
            topology.selectNode(ByteBuffer.wrap(key), nodesPerSample)
                    .forEach(
                            node -> nodesCounters.compute(node, (__, count) -> count == null ? 1 : count + 1)
                    );
        }

        final var expectedCount = iterationsCount / topology.nodesAmount() * nodesPerSample;
        final var results = nodesCounters.values().stream()
                .map(Integer::doubleValue)
                .map(count -> count / expectedCount)
                .map(count -> count > 1 ? count - 1 : 1 - count);
        assertTrue(results.allMatch(count -> count < possibleDeviation));
    }

    @Test
    void testDistributionAfterNodeAddition() {
        testDistributionAfterNodeAddition(1);
    }

    @Test
    void testDistributionWithReplicasAfterNodeAddition() {
        testDistributionAfterNodeAddition(3);
    }

    void testDistributionAfterNodeAddition(final int nodesPerSample) {
        final var iterationsCount = 100_000;
        final var possibleMistakesPercents = 0.02 * nodesPerSample;

        final var topology = createTopology(100);
        final var extendedTopology = topology.addNode(101);

        double mistakesCount = 0;
        for (int i = 0; i < iterationsCount; i++) {
            final var key = randomId().getBytes(StandardCharsets.UTF_8);
            final var original = topology.selectNode(ByteBuffer.wrap(key), nodesPerSample);
            final var extended = extendedTopology.selectNode(ByteBuffer.wrap(key), nodesPerSample);
            mistakesCount += extended.stream().filter(it -> !original.contains(it)).count();
        }
        assertTrue(mistakesCount / iterationsCount < possibleMistakesPercents);
    }

}
