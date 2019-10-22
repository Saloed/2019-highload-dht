package ru.mail.polis.service.saloed;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.IOExceptionLight;
import ru.mail.polis.service.Service;

public final class ServiceImpl extends HttpServer implements Service {

    private static final Log log = LogFactory.getLog(ServiceImpl.class);
    private static final String TIMESTAMP_HEADER = "X-Service-Timestamp:";
    private static final String SERVICE_REQUEST_HEADER = "X-Service-Request:";
    private static final int TIMEOUT = 100;

    private final DAOWithTimestamp dao;
    private final Topology topology;
    private final Map<String, StreamHttpClient> pool;
    private final Map<Integer, EntityRequestProcessor> entityRequestProcessor;

    private ServiceImpl(final HttpServerConfig config, @NotNull final DAOWithTimestamp dao,
        @NotNull final Topology topology, @NotNull final Map<String, StreamHttpClient> pool)
        throws IOException {
        super(config);
        this.dao = dao;
        this.topology = topology;
        this.pool = pool;
        this.entityRequestProcessor = ImmutableMap.of(
            Request.METHOD_GET, EntityRequestProcessor.forHttpMethod(Request.METHOD_GET, dao),
            Request.METHOD_PUT, EntityRequestProcessor.forHttpMethod(Request.METHOD_PUT, dao),
            Request.METHOD_DELETE, EntityRequestProcessor.forHttpMethod(Request.METHOD_DELETE, dao)
        );

    }

    /**
     * Create service for specified port and DAO.
     *
     * @param port     port for incoming connections
     * @param dao      data access object
     * @param topology of cluster
     * @return created service
     * @throws IOException if something went wrong during server startup process
     */
    public static Service create(final int port, @NotNull final DAOWithTimestamp dao,
        @NotNull final Topology topology)
        throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.maxWorkers = Runtime.getRuntime().availableProcessors();
        final var pool = topology.allNodes().stream()
            .filter(node -> !topology.isMe(node))
            .collect(Collectors.toMap(
                node -> node,
                node -> new StreamHttpClient(new ConnectionString(node + "?timeout=" + TIMEOUT))
            ));
        return new ServiceImpl(config, dao, topology, pool);
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
     * @param id      key of entity
     * @param request HTTP request
     */
    @Path("/v0/entity")
    public void entity(
        @Param("id") final String id,
        @NotNull final Request request,
        @NotNull final HttpSession session) {
        if (id == null || id.isEmpty()) {
            response(session, Response.BAD_REQUEST, "Id is required");
            return;
        }
        final var method = request.getMethod();
        final var processor = entityRequestProcessor.get(method);
        if (processor == null) {
            response(session, Response.METHOD_NOT_ALLOWED);
            return;
        }
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final var body = method == Request.METHOD_PUT ? ByteBuffer.wrap(request.getBody()) : null;
        final var timestamp = getRequestTimestamp(request);
        final var isServiceRequest = isRequestFromService(request);
        final var nodeToProcess = topology.findNode(key);
        asyncExecute(() -> {
            try {
                Response response;
                if (isServiceRequest) {
                    response = processor.processForService(timestamp, key, body);
                } else if (topology.isMe(nodeToProcess)) {
                    response = processor.processForUser(timestamp, key, body);
                } else {
                    setRequestTimestamp(request, timestamp);
                    setRequestFromService(request);
                    final var serviceResponse = proxy(nodeToProcess, request);
                    response = processor.makeUserResponse(serviceResponse);
                }
                response(session, response);
            } catch (IOException ex) {
                log.error("Error while processing request", ex);
                response(session, Response.INTERNAL_ERROR);
            }
        });
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
            try {
                if (isServiceRequest) {
                    retrieveEntities(startBytes, finalEndBytes, streamSession);
                } else {
                    retrieveEntities(startBytes, finalEndBytes, streamSession, request);
                }
            } catch (IOException exception) {
                response(session, Response.INTERNAL_ERROR);
            }
        });
    }

    private void setRequestTimestamp(final Request request, final long timestamp) {
        request.addHeader(TIMESTAMP_HEADER + timestamp);
    }

    private void setRequestFromService(final Request request) {
        request.addHeader(SERVICE_REQUEST_HEADER + "true");
    }

    private boolean isRequestFromService(final Request request) {
        final var header = request.getHeader(SERVICE_REQUEST_HEADER);
        if (header == null) {
            return false;
        }
        return Boolean.parseBoolean(header);
    }

    private long getRequestTimestamp(final Request request) {
        final var header = request.getHeader(TIMESTAMP_HEADER);
        if (header == null) {
            return System.currentTimeMillis();
        }
        return Long.parseLong(header);
    }

    private void retrieveEntities(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final RecordStreamHttpSession streamSession,
        final Request request) throws IOException {
        final var iterators = new ArrayList<Iterator<Record>>();
        setRequestFromService(request);
        for (final var node : topology.allNodes()) {
            if (topology.isMe(node)) {
                final var iterator = dao.range(start, end);
                iterators.add(iterator);
                continue;
            }
            final var client = pool.get(node);
            try {
                final var iterator = client.invokeStream(request, (bytes) -> {
                    final var str = new String(bytes, StandardCharsets.UTF_8);
                    final var firstDelimiter = str.indexOf('\n');
                    final var keyStr = str.substring(0, firstDelimiter);
                    final var valueStr = str.substring(firstDelimiter);
                    final var keyBytes = ByteBuffer.wrap(keyStr.getBytes(StandardCharsets.UTF_8));
                    final var valueBytes = ByteBuffer
                        .wrap(valueStr.getBytes(StandardCharsets.UTF_8));
                    return Record.of(keyBytes, valueBytes);
                });
                if (iterator.getResponse().getStatus() != 200 || !iterator.isAvailable()) {
                    throw new IOExceptionLight("Unexpected response from node");
                }
                iterators.add(iterator);
            } catch (InterruptedException | HttpException | PoolException e) {
                throw new IOExceptionLight("Exception while entities request", e);
            }

        }
        final var mergedIterators = Iterators.mergeSorted(iterators, Record::compareTo);
        streamSession.stream(mergedIterators);
    }

    private void retrieveEntities(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final RecordStreamHttpSession streamSession) throws IOException {
        final var iterator = dao.range(start, end);
        streamSession.stream(iterator);
    }


    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new RecordStreamHttpSession(socket, this);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) {
        response(session, Response.BAD_REQUEST);
    }

    private Response proxy(final String node, final Request request) throws IOException {
        if (topology.isMe(node)) {
            throw new IllegalArgumentException("Self proxy");
        }
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOExceptionLight("Error in proxy", e);
        }
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
