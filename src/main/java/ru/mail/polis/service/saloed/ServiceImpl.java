package ru.mail.polis.service.saloed;

import com.google.common.collect.ImmutableMap;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.saloed.request.RequestUtils;
import ru.mail.polis.service.saloed.request.processor.EntitiesRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.entity.Arguments;
import ru.mail.polis.service.saloed.request.processor.entity.MaybeRecordWithTimestamp;
import ru.mail.polis.service.saloed.request.processor.entity.UpsertArguments;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ServiceImpl extends HttpServer implements Service {

    private static final Log log = LogFactory.getLog(ServiceImpl.class);

    private static final Pattern REPLICAS_PATTERN = Pattern.compile("(\\d+)/(\\d+)");

    private final DAOWithTimestamp dao;
    private final ClusterNodeRouter clusterNodeRouter;
    private final Map<Integer, EntityRequestProcessor> entityRequestProcessor;

    private ServiceImpl(final HttpServerConfig config, @NotNull final DAOWithTimestamp dao,
                        @NotNull final ClusterNodeRouter clusterNodeRouter)
            throws IOException {
        super(config);
        this.dao = dao;
        this.clusterNodeRouter = clusterNodeRouter;
        this.entityRequestProcessor = ImmutableMap.of(
                Request.METHOD_GET, EntityRequestProcessor.forHttpMethod(Request.METHOD_GET, dao),
                Request.METHOD_PUT, EntityRequestProcessor.forHttpMethod(Request.METHOD_PUT, dao),
                Request.METHOD_DELETE, EntityRequestProcessor.forHttpMethod(Request.METHOD_DELETE, dao)
        );
    }

    /**
     * Create service for specified port and DAO.
     *
     * @param port              port for incoming connections
     * @param dao               data access object
     * @param clusterNodeRouter of cluster
     * @return created service
     * @throws IOException if something went wrong during server startup process
     */
    public static Service create(final int port, @NotNull final DAOWithTimestamp dao,
                                 @NotNull final ClusterNodeRouter clusterNodeRouter)
            throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.maxWorkers = Runtime.getRuntime().availableProcessors();
        config.queueTime = 10; // ms
        return new ServiceImpl(config, dao, clusterNodeRouter);
    }

    private static int quorum(final int number) {
        return number / 2 + 1;
    }

    /**
     * Check status of this node.
     *
     * @return status of this node
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Provide access to entities.
     *
     * @param id       key of entity
     * @param replicas (optional) number of replicas to confirm request
     * @param request  HTTP request
     */
    @Path(EntityRequestProcessor.REQUEST_PATH)
    public void entity(
            @Param("id") final String id,
            @Param("replicas") final String replicas,
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        final var processor = entityRequestProcessor.get(request.getMethod());
        if (processor == null) {
            response(session, Response.METHOD_NOT_ALLOWED);
            return;
        }
        final var arguments = parseEntityArguments(id, replicas, request);
        if (arguments == null) {
            response(session, Response.BAD_REQUEST);
            return;
        }
        asyncExecute(() -> {
            if (arguments.isServiceRequest()) {
                processEntityForService(processor, arguments, session);
            } else {
                processEntityForUser(processor, arguments, session);
            }
        });
    }

    private void processEntityForService(
            final EntityRequestProcessor processor,
            final Arguments arguments,
            final HttpSession session) {
        final var result = processor.processLocal(arguments);
        if (result.isEmpty()) {
            log.error("Error while processing request");
            response(session, Response.INTERNAL_ERROR);
            return;
        }
        final var response = processor.makeResponseForService(result.get(), arguments);
        response(session, response);
    }

    private <T> CompletableFuture<List<T>> someOf(final List<CompletableFuture<Optional<T>>> futures, final int amount) {
        final var result = new CompletableFuture<List<T>>();
        final var resultValue = new CopyOnWriteArrayList<T>();
        final var newFutures = futures.stream().map(future -> future.thenAccept(value -> {
            value.ifPresent(resultValue::add);
            if (resultValue.size() >= amount) {
                result.complete(resultValue);
            }
        })).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(newFutures).thenAccept(r -> result.complete(resultValue)).exceptionally(ex -> {
            log.error("Exception during proxy", ex);
            return null;
        });
        return result;
    }

    private void processEntityForUser(
            final EntityRequestProcessor processor,
            final Arguments arguments,
            final HttpSession session) {
        final var nodes = clusterNodeRouter
                .selectNodes(arguments.getKey(), arguments.getReplicasFrom());
        final var remoteNodes = nodes.stream().filter(node -> !node.isLocal())
                .collect(Collectors.toList());
        final var localNode = nodes.stream().filter(node -> node.isLocal()).findAny();
        final var requestParams = ImmutableMap.of("id", arguments.getKeyString());
        final var remoteFutures = remoteNodes.stream()
                .map(node -> remoteRequest(node, processor, arguments, requestParams));
        final Optional<MaybeRecordWithTimestamp> localResult =
                localNode.isPresent() ? processor.processLocal(arguments) : Optional.empty();
        final var localFuture = CompletableFuture.completedFuture(localResult);
        final var futures = Stream.concat(remoteFutures, Stream.of(localFuture)).collect(Collectors.toList());
        final var results = someOf(futures, arguments.getReplicasAck());
        results.thenAccept(res -> {
            final var response = processor.makeResponseForUser(res, arguments);
            response(session, response);
        }).exceptionally(ex -> {
            log.error("Error while processing request", ex);
            response(session, Response.INTERNAL_ERROR);
            return null;
        });
    }

    @NotNull
    private CompletableFuture<Optional<MaybeRecordWithTimestamp>> remoteRequest(
            ClusterNodeRouter.ClusterNode node,
            EntityRequestProcessor processor,
            Arguments arguments,
            Map<String, String> requestParams) {
        final var requestBuilder = node.requestBuilder(EntityRequestProcessor.REQUEST_PATH, requestParams);
        final var request = processor.preprocessRemote(requestBuilder, arguments).build();
        return node.getHttpClient()
                .sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> processor.obtainRemoteResult(response, arguments))
                .completeOnTimeout(Optional.empty(), 100, TimeUnit.MILLISECONDS)
                .exceptionally(ex -> Optional.empty());
    }

    private Arguments parseEntityArguments(
            final String id,
            final String replicas,
            final Request request) {
        if (id == null || id.isEmpty()) {
            return null;
        }
        int replicasAck;
        int replicasFrom;
        final var matcher = REPLICAS_PATTERN.matcher(replicas == null ? "" : replicas);
        if (matcher.find()) {
            replicasAck = Integer.parseInt(matcher.group(1));
            replicasFrom = Integer.parseInt(matcher.group(2));
        } else {
            final var nodesCount = clusterNodeRouter.getNodesAmount();
            replicasAck = quorum(nodesCount);
            replicasFrom = nodesCount;
        }
        if (replicasFrom > clusterNodeRouter.getNodesAmount() || replicasAck > replicasFrom
                || replicasAck < 1) {
            return null;
        }
        final var timestamp = RequestUtils.getRequestTimestamp(request);
        final var isServiceRequest = RequestUtils.isRequestFromService(request);
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        if (request.getMethod() == Request.METHOD_PUT) {
            final var body = ByteBuffer.wrap(request.getBody());
            return new UpsertArguments(key, id, body, isServiceRequest, timestamp, replicasAck, replicasFrom);
        }
        return new Arguments(key, id, isServiceRequest, timestamp, replicasAck, replicasFrom);
    }

    /**
     * Retrieve all entities from start to end. If end is missing -- retrieve all entities up to the
     * end.
     *
     * @param start of key range (required)
     * @param end   of key range (optional)
     */
    @Path(EntitiesRequestProcessor.REQUEST_PATH)
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        final var isServiceRequest = RequestUtils.isRequestFromService(request);
        final var arguments = EntitiesRequestProcessor.Arguments.parse(start, end, isServiceRequest);
        if (arguments == null) {
            response(session, Response.BAD_REQUEST);
            return;
        }
        final var streamSession = (StreamHttpSession) session;
        asyncExecute(() -> {
            final var processor = new EntitiesRequestProcessor(clusterNodeRouter, dao);
            try {
                if (arguments.isServiceRequest()) {
                    processor.processForService(arguments, streamSession);
                } else {
                    processor.processForUser(arguments, streamSession);
                }
            } catch (IOException exception) {
                response(session, Response.INTERNAL_ERROR);
            }
        });
    }

    @Override
    public synchronized void stop() {
        clusterNodeRouter.close();
        super.stop();
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StreamHttpSession(socket, this);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) {
        response(session, Response.BAD_REQUEST);
    }

    private void response(final HttpSession session, final String resultCode) {
        response(session, resultCode, Response.EMPTY);
    }

    private void response(final HttpSession session, final String resultCode, final byte[] body) {
        final var response = new Response(resultCode, body);
        response(session, response);
    }

    private void response(final HttpSession session, final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException exception) {
            log.error("Error during send response", exception);
        }
    }

}
