package ru.mail.polis.service.saloed;

import static ru.mail.polis.service.saloed.RequestUtils.setRequestFromService;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
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
import one.nio.server.RejectedSessionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.saloed.EntityClusterTask.Arguments;
import ru.mail.polis.service.saloed.UpsertEntityClusterTask.UpsertArguments;

public final class ServiceImpl extends HttpServer implements Service {

    private static final Log log = LogFactory.getLog(ServiceImpl.class);

    private final DAOWithTimestamp dao;
    private final ClusterNodeRouter clusterNodeRouter;
    private final Map<Integer, EntityClusterTask> entityRequestProcessor;

    private ServiceImpl(final HttpServerConfig config, @NotNull final DAOWithTimestamp dao,
        @NotNull final ClusterNodeRouter clusterNodeRouter)
        throws IOException {
        super(config);
        this.dao = dao;
        this.clusterNodeRouter = clusterNodeRouter;
        this.entityRequestProcessor = ImmutableMap.of(
            Request.METHOD_GET, EntityClusterTask.forHttpMethod(Request.METHOD_GET, dao),
            Request.METHOD_PUT, EntityClusterTask.forHttpMethod(Request.METHOD_PUT, dao),
            Request.METHOD_DELETE, EntityClusterTask.forHttpMethod(Request.METHOD_DELETE, dao)
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
    @Path("/v0/entity")
    public void entity(
        @Param("id") final String id,
        @Param("replicas") final String replicas,
        @NotNull final Request request,
        @NotNull final HttpSession session) {
        if (id == null || id.isEmpty()) {
            response(session, Response.BAD_REQUEST, "Id is required");
            return;
        }
        final var processor = entityRequestProcessor.get(request.getMethod());
        if (processor == null) {
            response(session, Response.METHOD_NOT_ALLOWED);
            return;
        }
        final var arguments = parseEntityArguments(id, replicas, request);
        asyncExecute(() -> {
            try {
                final var response = clusterNodeRouter.executeClusterTask(
                    arguments.getKey(), arguments.getReplicasFrom(),
                    request, arguments, processor);
                response(session, response);
            } catch (IOException ex) {
                log.error("Error while processing request", ex);
                response(session, Response.INTERNAL_ERROR);
            }
        });
    }

    private EntityClusterTask.Arguments parseEntityArguments(
        final String id,
        final String replicas,
        final Request request) {
        final var timestamp = RequestUtils.getRequestTimestamp(request);
        final var isServiceRequest = RequestUtils.isRequestFromService(request);
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int replicasAck;
        int replicasFrom;
        final var matcher = Pattern.compile("(\\d+)/(\\d+)")
            .matcher(replicas == null ? "" : replicas);
        if (matcher.find()) {
            replicasAck = Integer.parseInt(matcher.group(1));
            replicasFrom = Integer.parseInt(matcher.group(2));
        } else {
            replicasAck = clusterNodeRouter.getDefaultReplicasAck();
            replicasFrom = clusterNodeRouter.getDefaultReplicasFrom();
        }
        if (request.getMethod() == Request.METHOD_PUT) {
            final var body = ByteBuffer.wrap(request.getBody());
            return new UpsertArguments(key, body, isServiceRequest, timestamp, replicasAck,
                replicasFrom);
        }
        return new Arguments(key, isServiceRequest, timestamp, replicasAck,
            replicasFrom);
    }

    /**
     * Retrieve all entities from start to end. If end is missing -- retrieve all entities up to the
     * end.
     *
     * @param start of key range (required)
     * @param end   of key range (optional)
     */
    @Path("/v0/entities")
    public void entities(
        @Param("start") final String start,
        @Param("end") final String end,
        @NotNull final Request request,
        @NotNull final HttpSession session) {
        if (start == null || start.isEmpty()) {
            response(session, Response.BAD_REQUEST, "Start parameter is required");
            return;
        }
        final var startBytes = ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));
        ByteBuffer endBytes = null;
        if (end != null && !end.isEmpty()) {
            endBytes = ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8));
        }
        final var finalEndBytes = endBytes;
        final var streamSession = (RecordStreamHttpSession) session;
        final var isServiceRequest = isRequestFromService(request);
        asyncExecute(() -> {
            final var processor = new EntitiesRequestProcessor(clusterNodeRouter, dao, pool);
            try {
                if (isServiceRequest) {
                    processor.processForService(startBytes, finalEndBytes, streamSession);
                } else {
                    setRequestFromService(request);
                    processor.processForUser(startBytes, finalEndBytes, request, streamSession);
                }
            } catch (IOException exception) {
                response(session, Response.INTERNAL_ERROR);
            }
        });
    }

    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new RecordStreamHttpSession(socket, this);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) {
        response(session, Response.BAD_REQUEST);
    }

    private void response(final HttpSession session, final String resultCode) {
        response(session, resultCode, Response.EMPTY);
    }

    private void response(final HttpSession session, final String resultCode,
        final String message) {
        final var messageBytes = message.getBytes(StandardCharsets.UTF_8);
        response(session, resultCode, messageBytes);
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
