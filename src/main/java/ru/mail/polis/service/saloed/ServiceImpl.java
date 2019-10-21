package ru.mail.polis.service.saloed;

import java.util.Map;
import java.util.stream.Collectors;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

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

import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;
import ru.mail.polis.service.Service;

public final class ServiceImpl extends HttpServer implements Service {

    private static final Log log = LogFactory.getLog(ServiceImpl.class);
    private static final String TIMESTAMP_HEADER = "X-Service-Timestamp:";
    private static final String SERVICE_REQUEST_HEADER = "X-Service-Request:";

    private final DAOWithTimestamp dao;
    private final Topology topology;
    private final Map<String, HttpClient> pool;

    private ServiceImpl(final HttpServerConfig config, @NotNull final DAOWithTimestamp dao,
        @NotNull final Topology topology, @NotNull final Map<String, HttpClient> pool)
        throws IOException {
        super(config);
        this.dao = dao;
        this.topology = topology;
        this.pool = pool;
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
                node -> new HttpClient(new ConnectionString(node + "?timeout=100"))
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
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final var timestamp = getRequestTimestamp(request);
        final var nodeToProcess = topology.findNode(key);
        if (isRequestFromService(request) || topology.isMe(nodeToProcess)) {
            asyncExecute(() -> {
                try {
                    dispatchEntityRequest(request, session, key, timestamp, method);
                } catch (IOException ex) {
                    response(session, Response.INTERNAL_ERROR);
                }
            });
            return;
        }
        setRequestTimestamp(request, timestamp);
        setRequestSelfProcess(request);
        asyncExecute(() -> {
            try {
                final var response = proxy(nodeToProcess, request);
                session.sendResponse(response);
            } catch (IOException ex) {
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
        asyncExecute(() -> {
            try {
                retrieveEntities(startBytes, finalEndBytes, streamSession);
            } catch (IOException exception) {
                response(session, Response.INTERNAL_ERROR);
            }
        });
    }

    private void dispatchEntityRequest(
        final Request request,
        final HttpSession session,
        final ByteBuffer key,
        final long timestamp,
        final int method) throws IOException {
        switch (method) {
            case Request.METHOD_GET: {
                getEntity(key, session);
                break;
            }
            case Request.METHOD_PUT: {
                putEntity(key, timestamp, request, session);
                break;
            }
            case Request.METHOD_DELETE: {
                deleteEntity(key, timestamp, session);
                break;
            }
            default: {
                response(session, Response.METHOD_NOT_ALLOWED);
                break;
            }
        }
    }

    private void setRequestTimestamp(final Request request, final long timestamp) {
        request.addHeader(TIMESTAMP_HEADER + timestamp);
    }

    private void setRequestSelfProcess(final Request request) {
        request.addHeader(SERVICE_REQUEST_HEADER + "true");
    }

    private boolean isRequestFromService(final Request request) {
        final var header = request.getHeader(SERVICE_REQUEST_HEADER);
        if (header == null) {
            return false;
        }
        final var value = header.substring(SERVICE_REQUEST_HEADER.length());
        return Boolean.parseBoolean(value);
    }

    private long getRequestTimestamp(final Request request) {
        final var header = request.getHeader(TIMESTAMP_HEADER);
        if (header == null) {
            return System.currentTimeMillis();
        }
        final var value = header.substring(TIMESTAMP_HEADER.length());
        return Long.parseLong(value);
    }

    private void retrieveEntities(
        @NotNull final ByteBuffer start,
        @Nullable final ByteBuffer end,
        final RecordStreamHttpSession streamSession) throws IOException {
        final var rangeIterator = dao.range(start, end);
        streamSession.stream(rangeIterator);
    }

    private void getEntity(final ByteBuffer key, final HttpSession session)
        throws IOException {
        try {
            final RecordWithTimestamp record = dao.getRecord(key);
            if (record.isValue()) {
                final var valueArray = ByteBufferUtils.toArray(record.getValue());
                response(session, Response.OK, valueArray);
            } else {
                response(session, Response.NOT_FOUND);
            }
        } catch (NoSuchElementException ex) {
            response(session, Response.NOT_FOUND);
        }
    }

    private void putEntity(final ByteBuffer key, long timestamp, final Request request,
        final HttpSession session)
        throws IOException {
        final var value = ByteBuffer.wrap(request.getBody());
        final var record = RecordWithTimestamp.fromValue(value, timestamp);
        dao.upsertRecord(key, record);
        response(session, Response.CREATED);
    }

    private void deleteEntity(final ByteBuffer key, long timestamp, final HttpSession session)
        throws IOException {
        final var record = RecordWithTimestamp.tombstone(timestamp);
        dao.upsertRecord(key, record);
        response(session, Response.ACCEPTED);
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
            throw new IOException("Error in proxy", e);
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
        try {
            session.sendResponse(response);
        } catch (IOException exception) {
            log.error("Error during send response", exception);
        }
    }

}
