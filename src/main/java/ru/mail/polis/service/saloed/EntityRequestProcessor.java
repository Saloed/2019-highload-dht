package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.nio.ByteBuffer;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

class EntityRequestProcessor {

    private final Operation operation;
    private final DAOWithTimestamp dao;

    private EntityRequestProcessor(final Operation operation, final DAOWithTimestamp dao) {
        this.operation = operation;
        this.dao = dao;
    }

    public static EntityRequestProcessor forHttpMethod(
        final int method,
        final DAOWithTimestamp dao) {
        switch (method) {
            case Request.METHOD_GET:
                return new EntityRequestProcessor(Operation.GET, dao);
            case Request.METHOD_PUT:
                return new EntityRequestProcessor(Operation.UPSERT, dao);
            case Request.METHOD_DELETE:
                return new EntityRequestProcessor(Operation.DELETE, dao);
            default:
                throw new IllegalArgumentException(
                    "Processor for method is unavailable: " + method);
        }
    }


    public Response processForService(
        final long timestamp,
        @NotNull final ByteBuffer key,
        @Nullable final ByteBuffer requestBody) throws IOException {
        return process(RequestType.SERVICE, timestamp, key, requestBody);
    }


    public Response processForUser(
        final long timestamp,
        @NotNull final ByteBuffer key,
        @Nullable final ByteBuffer requestBody) throws IOException {
        return process(RequestType.USER, timestamp, key, requestBody);
    }

    public Response makeUserResponse(final Response serviceResponse) {
        if (operation != Operation.GET || serviceResponse.getStatus() != 200) {
            return serviceResponse;
        }
        final var record = RecordWithTimestamp.fromBytes(serviceResponse.getBody());
        return responseForUserGet(record);
    }

    private Response process(
        final RequestType requestType,
        final long timestamp,
        @NotNull final ByteBuffer key,
        @Nullable final ByteBuffer requestBody) throws IOException {

        switch (operation) {
            case GET: {
                final var record = dao.getRecord(key);
                if (requestType == RequestType.USER) {
                    return responseForUserGet(record);
                }
                return new Response(Response.OK, record.toRawBytes());
            }
            case DELETE: {
                final var record = RecordWithTimestamp.tombstone(timestamp);
                dao.upsertRecord(key, record);
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
            case UPSERT: {
                if (requestBody == null) {
                    throw new IllegalArgumentException("Request body is null for UPSERT");
                }
                final var record = RecordWithTimestamp.fromValue(requestBody, timestamp);
                dao.upsertRecord(key, record);
                return new Response(Response.CREATED, Response.EMPTY);
            }
            default: {
                throw new IllegalStateException("Impossible (switch over enum)");
            }
        }
    }

    private Response responseForUserGet(final RecordWithTimestamp record) {
        if (record.isValue()) {
            final var valueArray = ByteBufferUtils.toArray(record.getValue());
            return new Response(Response.OK, valueArray);
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private enum Operation {
        GET, DELETE, UPSERT
    }

    private enum RequestType {
        USER, SERVICE
    }

}
