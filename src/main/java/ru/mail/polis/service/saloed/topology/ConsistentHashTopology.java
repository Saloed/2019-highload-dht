package ru.mail.polis.service.saloed.topology;

import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class ConsistentHashTopology<T> implements Topology<T> {

    private static final int PART_SIZE = 1 << 22;
    private static final int PARTS_NUMBER = 1 << (Integer.SIZE - 22);
    private final List<T> nodes;
    private final RangeMap<Integer, T> nodesTable;

    public ConsistentHashTopology(final List<T> nodes) {
        this.nodes = nodes;
        this.nodesTable = initializeTable(nodes);
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
    public T selectNode(@NotNull final ByteBuffer key) {
        final var keyHash = hash(key);
        final var node = nodesTable.get(keyHash);
        if (node == null) {
            throw new IllegalStateException("No entry for key in table");
        }
        return node;
    }
}
