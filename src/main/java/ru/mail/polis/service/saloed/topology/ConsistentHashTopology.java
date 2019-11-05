package ru.mail.polis.service.saloed.topology;

import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeRangeMap;
import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;

public class ConsistentHashTopology<T> implements Topology<T> {

    private static final int PART_SIZE = 1 << 22;
    private static final int PARTS_NUMBER = 1 << (Integer.SIZE - 22);
    private final List<T> nodes;
    private final List<NodeWithNext<T>> nodesWithNext;
    private final RangeMap<Integer, NodeWithNext<T>> nodesTable;

    public ConsistentHashTopology(final List<T> nodes) {
        this.nodes = nodes;
        this.nodesWithNext = initializeNext(nodes);
        this.nodesTable = initializeTable(nodesWithNext);
    }

    private static <T> RangeMap<Integer, T> initializeTable(final List<T> nodes) {
        final RangeMap<Integer, T> table = TreeRangeMap.create();
        final var nodeIterator = Iterators.cycle(nodes);
        for (int i = 0; i < PARTS_NUMBER; i++) {
            final var node = nodeIterator.next();
            final var lowerBound = Integer.MIN_VALUE + i * PART_SIZE;
            final var upperBound = Integer.MIN_VALUE + (i + 1) * PART_SIZE - 1;
            final var key = Range.closed(lowerBound, upperBound);
            table.put(key, node);
        }
        return table;
    }

    private static <T> List<NodeWithNext<T>> initializeNext(final List<T> nodes) {
        final var nodesWithNext = nodes.stream().map(NodeWithNext::new)
            .collect(Collectors.toList());
        final var nextStream = Streams.concat(
            nodesWithNext.subList(1, nodesWithNext.size()).stream(),
            Stream.of(nodesWithNext.get(0)));
        final var currentStream = nodesWithNext.stream();
        Streams.forEachPair(
            currentStream,
            nextStream,
            (current, next) -> current.next = next
        );
        return nodesWithNext;
    }

    private int hash(final ByteBuffer key) {
        final var keyCopy = key.duplicate();
        return Hashing.sha256()
            .newHasher(keyCopy.remaining())
            .putBytes(keyCopy)
            .hash()
            .asInt();
    }

    @Override
    public List<T> allNodes() {
        return new ArrayList<>(nodes);
    }

    @Override
    public int nodesAmount() {
        return nodes.size();
    }

    @Override
    public List<T> selectNode(@NotNull final ByteBuffer key, int replicas) {
        final var keyHash = hash(key);
        final var node = nodesTable.get(keyHash);
        if (node == null) {
            throw new IllegalStateException("No entry for key in table");
        }
        return getReplicasForNode(node, replicas);
    }

    @Override
    public Topology<T> addNode(T node) {
        final var newNode = new NodeWithNext<>(node);
        final var currentNodes = initializeNext(nodes);
        currentNodes.get(currentNodes.size() - 1).next = newNode;
        newNode.next = currentNodes.get(0);
        currentNodes.add(newNode);
        final RangeMap<Integer, NodeWithNext<T>> table = TreeRangeMap.create();
        int i = 0;
        for (final var entry : nodesTable.asMapOfRanges().entrySet()) {
            if (i % currentNodes.size() != 0) {
                table.put(entry.getKey(), entry.getValue());
            } else {
                table.put(entry.getKey(), newNode);
            }
            i++;
        }

        return null;
    }

    private List<T> getReplicasForNode(final NodeWithNext<T> rootNode, final int replicas) {
        if (replicas > nodesAmount() || replicas < 0) {
            throw new IllegalArgumentException("Incorrect number of replicas requested");
        }
        final var result = new ArrayList<T>(replicas);
        var current = rootNode;
        for (int i = 0; i < replicas; i++) {
            result.add(current.node);
            current = current.next;
        }
        return result;
    }

    private static final class NodeWithNext<T> {

        final T node;
        NodeWithNext<T> next;

        NodeWithNext(final T node) {
            this.node = node;
        }

    }

}
