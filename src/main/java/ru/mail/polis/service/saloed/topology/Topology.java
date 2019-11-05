package ru.mail.polis.service.saloed.topology;

import java.nio.ByteBuffer;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public interface Topology<T> {

    /**
     * Retrieve all known nodes.
     *
     * @return all nodes
     */
    List<T> allNodes();

    /**
     * Retrieve number of all known nodes.
     *
     * @return nodes amount
     */
    int nodesAmount();

    /**
     * Find node to serve request for given key.
     *
     * @param key      of request
     * @param replicas for request
     * @return nodes
     */
    List<T> selectNode(@NotNull final ByteBuffer key, int replicas);

    /**
     * Returns new topology, that contains one more node.
     *
     * @param node to add
     * @return topology
     */
    Topology<T> addNode(final T node);

}
