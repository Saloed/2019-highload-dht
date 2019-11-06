package ru.mail.polis.service.saloed;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        final var threadFactory = new ThreadFactoryBuilder().setNameFormat("node-router").build();
        final var workersPool = Executors.newFixedThreadPool(topology.size() * 2, threadFactory);
        final var nodes = topology.stream()
            .sorted()
            .map(node -> {
                final var type = node.equals(me) ? ClusterNodeType.LOCAL : ClusterNodeType.REMOTE;
                final var httpClient = createHttpClient(type, workersPool);
                return new ClusterNode(type, httpClient, node);
            })
            .collect(Collectors.toList());
        final var clusterTopology = new ConsistentHashTopology<>(nodes);
        return new ClusterNodeRouter(clusterTopology, workersPool);
    }

    private static HttpClient createHttpClient(final ClusterNodeType type, final Executor executor) {
        if (type == ClusterNodeType.LOCAL) {
            return null;
        }
        return HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(TIMEOUT))
            .executor(executor)
            .build();
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


        public HttpRequest.Builder requestBuilder(final String path,
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
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClusterNode)) {
                return false;
            }
            ClusterNode that = (ClusterNode) o;
            return type == that.type && endpoint.equals(that.endpoint);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, endpoint);
        }

        @Override
        public int compareTo(@NotNull ClusterNode o) {
            return endpoint.compareTo(o.endpoint);
        }
    }
}
