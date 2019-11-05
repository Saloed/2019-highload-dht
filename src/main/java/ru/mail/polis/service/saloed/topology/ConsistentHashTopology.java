package ru.mail.polis.service.saloed.topology;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeRangeMap;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

public class ConsistentHashTopology<T extends Comparable<T>> implements Topology<T> {

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

    private ConsistentHashTopology(
            final List<T> nodes,
            final List<NodeWithNext<T>> nodesWithNext,
            final RangeMap<Integer, NodeWithNext<T>> nodesTable) {
        this.nodes = nodes;
        this.nodesWithNext = nodesWithNext;
        this.nodesTable = nodesTable;
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

    private static <T extends Comparable<T>> List<NodeWithNext<T>> initializeNext(final List<T> nodes) {
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
        final var newNodes = new ArrayList<>(nodes);
        newNodes.add(node);
        newNodes.sort(T::compareTo);
        final var currentNodes = initializeNext(newNodes);
        final var newNodeOptional = currentNodes.stream()
                .filter(it -> it.node.compareTo(node) == 0)
                .findFirst();
        if (newNodeOptional.isEmpty()) {
            throw new IllegalStateException("Something wrong with compareTo");
        }
        final var newNode = newNodeOptional.get();
        final var newTable = rearrangeNodes(nodesTable, currentNodes, newNode);
        return new ConsistentHashTopology<T>(newNodes, currentNodes, newTable);
    }

    private RangeMap<Integer, NodeWithNext<T>> rearrangeNodes(
            final RangeMap<Integer, NodeWithNext<T>> currentTable,
            final List<NodeWithNext<T>> allNodesWithNew,
            final NodeWithNext<T> newNode) {

        final var newNodeRanges = rearrangeRanges(currentTable, allNodesWithNew, newNode);
        final var nodesMapping = getNodesMappingForArrange(currentTable.asMapOfRanges().values(), allNodesWithNew, newNode);
        final RangeMap<Integer, NodeWithNext<T>> newTable = TreeRangeMap.create();
        for (final var entry : newNodeRanges) {
            final var node = nodesMapping.get(entry.getValue());
            if (node == null) {
                throw new IllegalStateException("Remapping for node must be present");
            }
            newTable.put(entry.getKey(), node);
        }
        return newTable;
    }

    private List<Map.Entry<Range<Integer>, NodeWithNext<T>>> rearrangeRanges(
            final RangeMap<Integer, NodeWithNext<T>> currentTable,
            final List<NodeWithNext<T>> allNodesWithNew,
            final NodeWithNext<T> newNode
    ) {
        final var nodesRanges = currentTable.asMapOfRanges().entrySet().stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue))
                .values()
                .stream()
                .map(ArrayDeque::new)
                .collect(Collectors.toList());
        final var avgRangesPerNode = nodesRanges.stream()
                .map(Queue::size)
                .reduce(Integer::sum)
                .orElse(0) / allNodesWithNew.size();

        final var newRanges = new ArrayList<Map.Entry<Range<Integer>, NodeWithNext<T>>>();
        int rangesPerNewNode = 0;
        while (rangesPerNewNode <= avgRangesPerNode) {
            final var nodeRanges = nodesRanges.stream().max(Comparator.comparing(ArrayDeque::size)).get();
            final var range = nodeRanges.poll();
            if (range == null) {
                continue;
            }
            newRanges.add(Maps.immutableEntry(range.getKey(), newNode));
            rangesPerNewNode++;
        }
        final var unchangedOldRanges = nodesRanges.stream().flatMap(Collection::stream).collect(Collectors.toList());
        newRanges.addAll(unchangedOldRanges);
        return newRanges;
    }

    private Map<NodeWithNext<T>, NodeWithNext<T>> getNodesMappingForArrange(
            final Collection<NodeWithNext<T>> oldNodes,
            final Collection<NodeWithNext<T>> newNodes,
            final NodeWithNext<T> additionalNode) {
        // ensure nodes are unique
        final var oldNodesSet = new HashSet<>(oldNodes);
        final var newNodesMap = new HashSet<>(newNodes).stream().collect(Collectors.toMap(it -> it.node, it -> it));

        final var nodesMapping = new HashMap<NodeWithNext<T>, NodeWithNext<T>>();
        for (final var node : oldNodesSet) {
            final var newNode = newNodesMap.get(node.node);
            if (newNode == null) {
                throw new IllegalStateException("Node must be present");
            }
            nodesMapping.put(node, newNode);
        }
        nodesMapping.put(additionalNode, additionalNode);
        return nodesMapping;
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

    private static final class NodeWithNext<T extends Comparable<T>> implements Comparable<NodeWithNext<T>> {

        final T node;
        NodeWithNext<T> next;

        NodeWithNext(final T node) {
            this.node = node;
        }

        @Override
        public int compareTo(@NotNull NodeWithNext<T> o) {
            return node.compareTo(o.node);
        }

        @Override
        public String toString() {
            return "NodeWithNext{" + node.toString() + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NodeWithNext)) return false;
            NodeWithNext<?> that = (NodeWithNext<?>) o;
            return node.equals(that.node);
        }

        @Override
        public int hashCode() {
            return Objects.hash(node);
        }
    }

}
