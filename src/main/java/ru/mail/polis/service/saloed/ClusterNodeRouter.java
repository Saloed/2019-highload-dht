package ru.mail.polis.service.saloed;

import com.google.common.collect.*;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.IOExceptionLight;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ClusterNodeRouter {
    private static final int TIMEOUT = 100;
    private static final int PART_SIZE = 1 << 22;
    private static final int PARTS_NUMBER = 1 << (Integer.SIZE - 22);
    private final List<ClusterNode> nodes;
    private final RangeMap<Integer, ClusterNode> nodesTable;
    private final ExecutorService workersPool;

    private ClusterNodeRouter(final List<ClusterNode> nodes, final ExecutorService workersPool) {
        this.nodes = nodes;
        this.nodesTable = initializeTable(nodes);
        this.workersPool = workersPool;
    }

    /**
     * Creates cluster topology for given node addresses and current node address.
     *
     * @param topology of cluster
     * @param me       current node address
     * @return topology
     */
    public static ClusterNodeRouter create(
            @NotNull final Set<String> topology,
            @NotNull final String me) {
        if (!topology.contains(me)) {
            throw new IllegalArgumentException("Me is not part of topology");
        }
        final var nodes = topology.stream()
                .sorted()
                .map(node -> {
                    final var type = node.equals(me) ? ClusterNodeType.LOCAL : ClusterNodeType.REMOTE;
                    final var httpClient = createHttpClient(type, node);
                    return new ClusterNode(type, httpClient);
                })
                .collect(Collectors.toList());
        final var nextStream = Streams.concat(
                nodes.subList(1, nodes.size()).stream(),
                Stream.of(nodes.get(0)));
        final var currentStream = nodes.stream();
        final var nodesChained = Streams.zip(
                currentStream,
                nextStream,
                (current, next) -> current.next = next)
                .collect(Collectors.toList());

        final var threadFactory = new ThreadFactoryBuilder().setNameFormat("node-router").build();
        final var workersPool = Executors.newFixedThreadPool(nodes.size(), threadFactory);
        return new ClusterNodeRouter(nodesChained, workersPool);
    }

    private static StreamHttpClient createHttpClient(final ClusterNodeType type,
                                                     final String node) {
        if (type == ClusterNodeType.LOCAL) {
            return null;
        }
        final var connectionStr = new ConnectionString(node + "?timeout=" + TIMEOUT);
        return new StreamHttpClient(connectionStr);
    }

    private static RangeMap<Integer, ClusterNode> initializeTable(final List<ClusterNode> nodes) {
        final RangeMap<Integer, ClusterNode> table = TreeRangeMap.create();
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

    private Response proxySingleRequest(
            final ClusterNode node,
            final Request request
    ) throws IOException {
        try {
            return node.getHttpClient().invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOExceptionLight("Error in proxy", e);
        }
    }

    public Future<Response> proxyRequest(final ClusterNode node, final Request request) {
        return workersPool.submit(() -> proxySingleRequest(node, request));
    }

    List<Future<Response>> proxyRequest(final List<ClusterNode> nodes, final Request request) {
        return nodes.stream()
                .filter(node -> !node.isLocal())
                .map(node -> this.workersPool.submit(() -> proxySingleRequest(node, request)))
                .collect(Collectors.toList());
    }


    int getDefaultReplicasAck() {
        return nodes.size() / 2 + 1;
    }

    int getDefaultReplicasFrom() {
        return nodes.size();
    }

    /**
     * Retrieve all known nodes.
     *
     * @return all nodes
     */
    List<ClusterNode> allNodes() {
        return new ArrayList<>(nodes);
    }

    /**
     * Find nodes to serve request for given key.
     *
     * @param key      of request
     * @param replicas number of replicas
     * @return nodes
     */
    List<ClusterNode> selectNodes(@NotNull final ByteBuffer key, final int replicas) {
        final var keyHash = hash(key);
        final var node = nodesTable.get(keyHash);
        if (node == null) {
            throw new IllegalStateException("No entry for key in table");
        }
        return getReplicasForNode(node, replicas);
    }

    private List<ClusterNode> getReplicasForNode(final ClusterNode targetNode, final int replicas) {
        if (replicas > nodes.size()) {
            throw new IllegalArgumentException("Too much replicas requested");
        }
        final var result = new ArrayList<ClusterNode>(replicas);
        int nodeCount = 0;
        for (final var node : targetNode) {
            if (nodeCount >= replicas) {
                break;
            }
            result.add(node);
        }
        return result;
    }

    private enum ClusterNodeType {
        LOCAL, REMOTE
    }

    public final static class ClusterNode implements Iterable<ClusterNode> {

        private final ClusterNodeType type;
        private final StreamHttpClient httpClient;
        private ClusterNode next;

        ClusterNode(final ClusterNodeType type, final StreamHttpClient httpClient) {
            this.type = type;
            this.httpClient = httpClient;
        }

        boolean isLocal() {
            return type == ClusterNodeType.LOCAL;
        }

        @NotNull
        @Override
        public Iterator<ClusterNode> iterator() {
            return new ClusterNodeIterator(this);
        }

        StreamHttpClient getHttpClient() {
            return httpClient;
        }

        private static final class ClusterNodeIterator implements Iterator<ClusterNode> {

            private ClusterNode node;

            ClusterNodeIterator(final ClusterNode node) {
                this.node = node;
            }

            @Override
            public boolean hasNext() {
                return node != null;
            }

            @Override
            public ClusterNode next() {
                final var current = node;
                node = node.next;
                return current;
            }
        }
    }
}
