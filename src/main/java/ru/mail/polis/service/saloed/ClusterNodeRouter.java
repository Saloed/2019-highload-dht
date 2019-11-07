package ru.mail.polis.service.saloed;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.Closeable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.saloed.topology.ConsistentHashTopology;
import ru.mail.polis.service.saloed.topology.Topology;

public final class ClusterNodeRouter implements Closeable {

    private static final int TIMEOUT = 100;
    private final ExecutorService workersPool;
    private final Topology<ClusterNode> topology;

    private ClusterNodeRouter(final Topology<ClusterNode> topology,
        final ForkJoinPool workersPool) {
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
        final var workersPool = new ForkJoinPool(
            topology.size(),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null,
            true);
        final var client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(TIMEOUT))
            .executor(workersPool)
            .build();
        final var nodes = topology.stream()
            .sorted()
            .map(node -> {
                final var type = node.equals(me) ? ClusterNodeType.LOCAL : ClusterNodeType.REMOTE;
                return new ClusterNode(type, client, node);
            })
            .collect(Collectors.toList());
        final var clusterTopology = ConsistentHashTopology.forNodes(nodes);
        return new ClusterNodeRouter(clusterTopology, workersPool);
    }

    ExecutorService getWorkers() {
        return workersPool;
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
        return topology.selectNode(key, replicas);
    }

    @Override
    public void close() {
        MoreExecutors.shutdownAndAwaitTermination(workersPool, TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private enum ClusterNodeType {
        LOCAL, REMOTE
    }

    public static final class ClusterNode implements Comparable<ClusterNode> {

        private final ClusterNodeType type;
        private final HttpClient httpClient;
        private final String endpoint;

        ClusterNode(final ClusterNodeType type, final HttpClient httpClient,
            final String endpoint) {
            this.type = type;
            this.httpClient = httpClient;
            this.endpoint = endpoint;
        }

        /**
         * Check whether node is local (current) node.
         *
         * @return result of check
         */
        public boolean isLocal() {
            return type == ClusterNodeType.LOCAL;
        }

        /**
         * Get http client for node. Returns null for local node.
         *
         * @return http client
         */
        public HttpClient getHttpClient() {
            return httpClient;
        }

        /**
         * Return request builder for current node with given path and request params.
         *
         * @param path   of HTTP resource
         * @param params of request
         * @return request builder
         */
        public HttpRequest.Builder requestBuilder(
            final String path,
            final Map<String, String> params) {
            String paramsStr = "";
            if (!params.isEmpty()) {
                paramsStr = "?" + params.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining("&"));
            }
            final var requestUrl = URI.create(endpoint + path + paramsStr);
            return HttpRequest.newBuilder(requestUrl).timeout(Duration.ofMillis(TIMEOUT));
        }

        @Override
        public String toString() {
            return "ClusterNode{" + type.name() + " " + endpoint + '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClusterNode)) {
                return false;
            }
            final ClusterNode that = (ClusterNode) o;
            return type == that.type && endpoint.equals(that.endpoint);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, endpoint);
        }

        @Override
        public int compareTo(@NotNull final ClusterNode o) {
            return endpoint.compareTo(o.endpoint);
        }
    }
}
