package ru.mail.polis.service.saloed;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
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
import ru.mail.polis.service.saloed.payload.Payload;
import ru.mail.polis.service.saloed.request.RequestUtils;
import ru.mail.polis.service.saloed.request.handler.RejectedRequestHandler;
import ru.mail.polis.service.saloed.request.handler.RequestHandler;
import ru.mail.polis.service.saloed.request.processor.EntitiesRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.entity.Arguments;
import ru.mail.polis.service.saloed.request.processor.entity.MaybeRecordWithTimestamp;
import ru.mail.polis.service.saloed.request.processor.entity.UpsertArguments;

public final class ServiceImpl extends HttpServer implements Service {

    private static final Log log = LogFactory.getLog(ServiceImpl.class);

    private static final Pattern REPLICAS_PATTERN = Pattern.compile("(\\d+)/(\\d+)");

    private final DAOWithTimestamp dao;
    private final ClusterNodeRouter clusterNodeRouter;
    private final ServiceMetrics metrics;

    private ServiceImpl(final HttpServerConfig config, @NotNull final DAOWithTimestamp dao,
        @NotNull final ClusterNodeRouter clusterNodeRouter)
        throws IOException {
        super(config);
        this.dao = dao;
        this.clusterNodeRouter = clusterNodeRouter;
        this.metrics = new ServiceMetrics(clusterNodeRouter, this);
        final var myWorkers = (ThreadPoolExecutor) workers;
        final var currentHandler = myWorkers.getRejectedExecutionHandler();
        final var rejectedRequestsHandler = new RejectedRequestHandler(currentHandler);
        myWorkers.setRejectedExecutionHandler(rejectedRequestsHandler);
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
        metrics.request();
        final var arguments = parseEntityArguments(id, replicas, request);
        if (arguments == null) {
            response(session, Response.BAD_REQUEST);
            metrics.successResponse();
            return;
        }
        final var processor = EntityRequestProcessor
            .forHttpMethod(request.getMethod(), dao, arguments);
        if (processor == null) {
            response(session, Response.METHOD_NOT_ALLOWED);
            metrics.successResponse();
            return;
        }
        final var handler = new RequestHandler(metrics, session, () -> {
            if (arguments.isServiceRequest()) {
                processEntityForService(processor, session);
            } else {
                processEntityForUser(processor, session);
            }
        });
        asyncExecute(handler);
    }

    private void processEntityForService(
        final EntityRequestProcessor processor,
        final HttpSession session) {
        metrics.serviceRequest();
        final var result = processor.processLocal();
        if (result.isEmpty()) {
            log.error("Error while processing request");
            response(session, Response.INTERNAL_ERROR);
            metrics.errorServiceResponse();
            return;
        }
        final var response = processor.makeResponseForService(result.get());
        response(session, response);
        metrics.successServiceResponse();
    }

    private <T> CompletableFuture<Collection<T>> someOf(
        final List<CompletableFuture<Optional<T>>> futures,
        final int amount,
        final int from) {
        final var result = new CompletableFuture<Collection<T>>();
        final var resultValue = new ConcurrentLinkedQueue<T>();
        final var errorCounter = new AtomicInteger(from - amount);
        for (final var future : futures) {
            future
                .thenAccept(value -> {
                    value.ifPresentOrElse(resultValue::add, errorCounter::decrementAndGet);
                    if (resultValue.size() >= amount || errorCounter.get() < 0) {
                        result.complete(resultValue);
                    }
                })
                .exceptionally(ex -> {
                    result.completeExceptionally(ex);
                    return null;
                });
        }
        return result;
    }

    private void processEntityForUser(
        final EntityRequestProcessor processor,
        final HttpSession session) {
        metrics.userRequest();
        final var arguments = processor.arguments;
        final var nodes = clusterNodeRouter
            .selectNodes(arguments.getKey(), arguments.getReplicasFrom());
        final var requestParams = ImmutableMap.of("id", arguments.getKeyString());
        final var resultFutures = new ArrayList<CompletableFuture<Optional<MaybeRecordWithTimestamp>>>();
        boolean needProcessLocally = false;
        for (final var node : nodes) {
            if (node.isLocal()) {
                needProcessLocally = true;
                continue;
            }
            final var request = remoteRequest(node, processor, requestParams);
            resultFutures.add(request);
        }
        if (needProcessLocally) {
            final var localResult = processor.processLocal();
            final var localFuture = CompletableFuture.completedFuture(localResult);
            resultFutures.add(localFuture);
        }
        someOf(resultFutures, arguments.getReplicasAck(), arguments.getReplicasFrom())
            .thenAccept(res -> {
                final var response = processor.makeResponseForUser(res);
                response(session, response);
                metrics.successUserResponse();
            })
            .exceptionally(ex -> {
                log.error("Error while processing request", ex);
                response(session, Response.INTERNAL_ERROR);
                metrics.errorUserResponse();
                return null;
            });
    }

    @NotNull
    private CompletableFuture<Optional<MaybeRecordWithTimestamp>> remoteRequest(
        final ClusterNodeRouter.ClusterNode node,
        final EntityRequestProcessor processor,
        final Map<String, String> requestParams) {
        final var requestBuilder = node
            .requestBuilder(EntityRequestProcessor.REQUEST_PATH, requestParams);
        final var request = processor.preprocessRemote(requestBuilder).build();
        return node.getHttpClient()
            .sendAsync(request, processor::obtainRemoteResult)
            .thenApply(HttpResponse::body)
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
            return new UpsertArguments(key, id, body, isServiceRequest, timestamp, replicasAck,
                replicasFrom);
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
        metrics.request();
        final var isServiceRequest = RequestUtils.isRequestFromService(request);
        final var arguments = EntitiesRequestProcessor.Arguments
            .parse(start, end, isServiceRequest);
        if (arguments == null) {
            response(session, Response.BAD_REQUEST);
            return;
        }
        final var streamSession = (StreamHttpSession) session;
        final var handler = new RequestHandler(metrics, session, () -> {
            final var processor = new EntitiesRequestProcessor(clusterNodeRouter, dao);
            metrics.request(arguments.isServiceRequest());
            final var result = arguments.isServiceRequest()
                ? processor.processForService(arguments)
                : processor.processForUser(arguments);
            result
                .thenAccept(recordIterator -> response(streamSession, recordIterator))
                .exceptionally(__ -> {
                    metrics.errorResponse();
                    response(streamSession, Response.INTERNAL_ERROR);
                    return null;
                });
        });
        asyncExecute(handler);
    }

    @Override
    public synchronized void stop() {
        clusterNodeRouter.close();
        metrics.close();
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

    private void response(final StreamHttpSession session, final Iterator<Payload> data) {
        metrics.responseWithStatus(200);
        try {
            session.stream(data);
        } catch (IOException exception) {
            session.close();
            metrics.errorResponse();
            log.error("Error during stream response", exception);
        }
    }

    private void response(final HttpSession session, final Response response) {
        metrics.responseWithStatus(response.getStatus());
        try {
            session.sendResponse(response);
        } catch (IOException exception) {
            session.close();
            metrics.errorResponse();
            log.error("Error during send response", exception);
        }
    }

}
