package ru.mail.polis.service.saloed;

import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Topology {

    private final String current;
    private final List<String> nodes;
    private final RangeMap<Integer, String> nodesTable;
    private static final int PART_SIZE = 1 << 22;
    private static final int PARTS_NUMBER = 1 << (Integer.SIZE - 22);

    private Topology(final List<String> nodes, final String currentNode) {
        this.nodes = nodes;
        this.current = currentNode;
        nodesTable = initializeTable();
    }

    /**
     * Creates cluster topology for given node addresses and current node address.
     *
     * @param topology of cluster
     * @param me       current node address
     * @return topology
     */
    public static Topology create(@NotNull final Set<String> topology, @NotNull final String me) {
        if (!topology.contains(me)) {
            throw new IllegalArgumentException("Me is not part of topology");
        }
        final var nodes = topology.stream().sorted().collect(Collectors.toList());
        return new Topology(nodes, me);
    }

    private int hash(final ByteBuffer key) {
        final var keyCopy = key.duplicate();
        return Hashing.sha256()
                .newHasher(keyCopy.remaining())
                .putBytes(keyCopy)
                .hash()
                .asInt();
    }

    private RangeMap<Integer, String> initializeTable() {
        final RangeMap<Integer, String> table = TreeRangeMap.create();
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

    /**
     * Check whether given node is current node.
     *
     * @param node to check
     * @return check result
     */
    public boolean isMe(@NotNull final String node) {
        return current.equals(node);
    }

    /**
     * Retrieve all known nodes.
     *
     * @return all nodes
     */
    public Set<String> allNodes() {
        return new HashSet<>(nodes);
    }

    /**
     * Find node to serve request for given key.
     *
     * @param key of request
     * @return node
     */
    public String findNode(@NotNull final ByteBuffer key) {
        final var keyHash = hash(key);
        final var node = nodesTable.get(keyHash);
        if (node == null) {
            throw new IllegalStateException("No entry for key in table");
        }
        return node;
    }

}
