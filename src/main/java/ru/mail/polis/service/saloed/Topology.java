package ru.mail.polis.service.saloed;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public class Topology {

    private final String current;
    private final List<String> nodes;

    private Topology(final List<String> nodes, final String currentNode) {
        this.nodes = nodes;
        this.current = currentNode;
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
        final var nodeId = (key.hashCode() & Integer.MAX_VALUE) % nodes.size();
        return nodes.get(nodeId);
    }

}
