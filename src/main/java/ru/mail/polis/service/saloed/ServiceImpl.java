package ru.mail.polis.service.saloed;

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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.slf4j.LoggerFactory;

import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;


public final class ServiceImpl extends HttpServer implements Service {
    private final DAO dao;

    private ServiceImpl(final HttpServerConfig config, @NotNull final DAO dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    /**
     * Create service for specified port and DAO.
     *
     * @param port -- port for incoming connections
     * @param dao  -- data access object
     * @return created service
     * @throws IOException if something went wrong during server startup process
     */
    public static Service create(final int port, @NotNull final DAO dao) throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.maxWorkers = Runtime.getRuntime().availableProcessors();
        return new ServiceImpl(config, dao);
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
     * @param id      -- key of entity
     * @param request -- HTTP request
     */
    @Path("/v0/entity")
    public void entity(
            @Param("id") final String id,
            @NotNull final Request request,
            @NotNull final HttpSession session
    ) {
        if (id == null || id.isEmpty()) {
            response(session, Response.BAD_REQUEST, "Id is required");
            return;
        }
        final var method = request.getMethod();
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        asyncExecute(() -> {
            try {
                switch (method) {
                    case Request.METHOD_GET:
                        getEntity(key, session);
                        return;
                    case Request.METHOD_PUT:
                        putEntity(key, request, session);
                        return;
                    case Request.METHOD_DELETE:
                        deleteEntity(key, session);
                        return;
                    default:
                        response(session, Response.METHOD_NOT_ALLOWED);
                }
            } catch (IOException ex) {
                response(session, Response.INTERNAL_ERROR);
            }
        });
    }

    /**
     * Retrieve all entities from start to end.
     * If end is missing -- retrieve all entities up to the end.
     *
     * @param start of key range (required)
     * @param end   of key range (optional)
     */
    @Path("/v0/entities")
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            @NotNull final HttpSession session
    ) {
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

    private void retrieveEntities(
            @NotNull final ByteBuffer start,
            @Nullable final ByteBuffer end,
            final RecordStreamHttpSession streamSession
    ) throws IOException {
        final var rangeIterator = dao.range(start, end);
        streamSession.stream(rangeIterator);
    }

    private void getEntity(final ByteBuffer key, final HttpSession session) throws IOException {
        try {
            final var value = dao.get(key).duplicate();
            final var valueArray = ByteBufferUtils.toArray(value);
            response(session, Response.OK, valueArray);
        } catch (NoSuchElementException ex) {
            response(session, Response.NOT_FOUND);
        }
    }

    private void putEntity(final ByteBuffer key, final Request request, final HttpSession session) throws IOException {
        final var value = ByteBuffer.wrap(request.getBody());
        dao.upsert(key, value);
        response(session, Response.CREATED);
    }

    private void deleteEntity(final ByteBuffer key, final HttpSession session) throws IOException {
        dao.remove(key);
        response(session, Response.ACCEPTED);
    }

    @Override
    public HttpSession createSession(Socket socket) throws RejectedSessionException {
        return new RecordStreamHttpSession(socket, this);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) {
        response(session, Response.BAD_REQUEST);
    }

    private void response(final HttpSession session, final String resultCode) {
        response(session, resultCode, Response.EMPTY);
    }

    private void response(final HttpSession session, final String resultCode, final String message) {
        final var messageBytes = message.getBytes(StandardCharsets.UTF_8);
        response(session, resultCode, messageBytes);
    }

    private void response(final HttpSession session, final String resultCode, final byte[] body) {
        final var response = new Response(resultCode, body);
        try {
            session.sendResponse(response);
        } catch (IOException exception) {
            final var log = LoggerFactory.getLogger(this.getClass());
            log.error("Error during send response", exception);
        }
    }

}
