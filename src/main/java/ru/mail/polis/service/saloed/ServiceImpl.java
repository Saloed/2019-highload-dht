package ru.mail.polis.service.saloed;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class ServiceImpl extends HttpServer implements Service {
    private final DAO dao;

    private ServiceImpl(final HttpServerConfig config, @NotNull final DAO dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    public static Service create(final int port, @NotNull final DAO dao) throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return new ServiceImpl(config, dao);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

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
                case Request.METHOD_GET: {
                    try {
                        final var value = dao.get(key).duplicate();
                        return Response.ok(value.array());
                    } catch (NoSuchElementException ex) {
                        return new Response(Response.NOT_FOUND, Response.EMPTY);
                    }
                }
                case Request.METHOD_PUT: {
                    final var value = ByteBuffer.wrap(request.getBody());
                    dao.upsert(key, value);
                    return new Response(Response.CREATED, Response.EMPTY);
                }
                case Request.METHOD_DELETE: {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                }
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (Exception ex) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }
}
