package ru.mail.polis.service.saloed;

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.IOExceptionLight;
import ru.mail.polis.service.saloed.topology.ConsistentHashTopology;
import ru.mail.polis.service.saloed.topology.Topology;

public final class ClusterNodeRouter implements Closeable {

    private static final Log log = LogFactory.getLog(ClusterNodeRouter.class);

    private static final int TIMEOUT = 100;
    private final ExecutorService workersPool;
    private final Topology<ClusterNode> topology;

    private ClusterNodeRouter(final Topology<ClusterNode> topology,
        final ExecutorService workersPool) {
        this.workersPool = workersPool;
        this.topology = topology;
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
        chainNodes(nodes);
        final var clusterTopology = new ConsistentHashTopology<>(nodes);
        final var threadFactory = new ThreadFactoryBuilder().setNameFormat("node-router").build();
        final var workersPool = Executors.newFixedThreadPool(nodes.size(), threadFactory);
        return new ClusterNodeRouter(clusterTopology, workersPool);
    }

    private static void chainNodes(final List<ClusterNode> nodes) {
        final var nextStream = Streams.concat(
            nodes.subList(1, nodes.size()).stream(),
            Stream.of(nodes.get(0)));
        final var currentStream = nodes.stream();
        Streams.forEachPair(
            currentStream,
            nextStream,
            (current, next) -> current.next = next
        );
    }

    private static StreamHttpClient createHttpClient(final ClusterNodeType type,
        final String node) {
        if (type == ClusterNodeType.LOCAL) {
            return null;
        }
        final var connectionStr = new ConnectionString(node + "?timeout=" + TIMEOUT);
        return new StreamHttpClient(connectionStr);
    }

    /**
     * Send HTTP request to given nodes asynchronously.
     *
     * @param nodes   receivers of request
     * @param request HTTP request
     * @return futures to responses
     */
    public List<Future<Response>> proxyRequest(final List<ClusterNode> nodes,
        final Request request) {
        return nodes.stream()
            .filter(node -> !node.isLocal())
            .map(node -> this.workersPool.submit(() -> proxySingleRequest(node, request)))
            .collect(Collectors.toList());
    }

    /**
     * Retrieve responses from corresponding futures with timeout.
     *
     * @param futures with responses
     * @return responses
     */
    public List<Response> obtainResponses(final List<Future<Response>> futures) {
        return futures.stream()
            .map(this::obtainResponse)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    private Optional<Response> obtainResponse(final Future<Response> responseFuture) {
        try {
            return Optional.of(responseFuture.get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Future get error", e);
            return Optional.empty();
        }
    }

    /**
     * Retrieve number of all known nodes.
     *
     * @return nodes amount
     */
    public int getNodesAmount() {
        return topology.nodesAmount();
    }

    /**
     * Retrieve all known nodes.
     *
     * @return all nodes
     */
    public List<ClusterNode> allNodes() {
        return topology.allNodes();
    }

    /**
     * Find nodes to serve request for given key.
     *
     * @param key      of request
     * @param replicas number of replicas
     * @return nodes
     */
    List<ClusterNode> selectNodes(@NotNull final ByteBuffer key, final int replicas) {
        final var node = topology.selectNode(key);
        return getReplicasForNode(node, replicas);
    }

    private List<ClusterNode> getReplicasForNode(final ClusterNode rootNode, final int replicas) {
        if (replicas > topology.nodesAmount()) {
            throw new IllegalArgumentException("Too much replicas requested");
        }
        final var result = new ArrayList<ClusterNode>(replicas);
        ClusterNode current = rootNode;
        for (int i = 0; i < replicas; i++) {
            result.add(current);
            current = current.next;
        }
        return result;
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

    @Override
    public void close() {
        MoreExecutors.shutdownAndAwaitTermination(workersPool, TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private enum ClusterNodeType {
        LOCAL, REMOTE
    }

    public final static class ClusterNode {

        private final ClusterNodeType type;
        private final StreamHttpClient httpClient;
        private ClusterNode next;

        ClusterNode(final ClusterNodeType type, final StreamHttpClient httpClient) {
            this.type = type;
            this.httpClient = httpClient;
        }

        public boolean isLocal() {
            return type == ClusterNodeType.LOCAL;
        }

        public StreamHttpClient getHttpClient() {
            return httpClient;
        }

    }
}
