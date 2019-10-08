package ru.mail.polis.service.saloed;

import org.jetbrains.annotations.NotNull;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;

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
     * Create service for specified port and DAO
     *
     * @param port -- port for incoming connections
     * @param dao  -- data access object
     * @return created service
     * @throws IOException
     */
    public static Service create(final int port, @NotNull final DAO dao) throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return new ServiceImpl(config, dao);
    }

    /**
     * Check status of this node
     *
     * @return status of this node
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Provide access to entities
     *
     * @param id      -- key of entity
     * @param request -- HTTP request
     * @return result or error response
     */
    @Path("/v0/entity")
    public Response entity(
            @Param("id") final String id,
            @NotNull final Request request
    ) {
        try {
            if (id == null || id.isEmpty()) {
                return new Response(Response.BAD_REQUEST, "Id is required".getBytes(StandardCharsets.UTF_8));
            }
            final var method = request.getMethod();
            final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
            switch (method) {
                case Request.METHOD_GET:
                    return getEntity(key);
                case Request.METHOD_PUT:
                    return putEntity(key, request);
                case Request.METHOD_DELETE:
                    return deleteEntity(key);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (Exception ex) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response getEntity(final ByteBuffer key) throws IOException {
        try {
            final var value = dao.get(key).duplicate();
            final var valueArray = ByteBufferUtils.toArray(value);
            return Response.ok(valueArray);
        } catch (NoSuchElementException ex) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response putEntity(final ByteBuffer key, final Request request) throws IOException {
        final var value = ByteBuffer.wrap(request.getBody());
        dao.upsert(key, value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response deleteEntity(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }


    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }
}
