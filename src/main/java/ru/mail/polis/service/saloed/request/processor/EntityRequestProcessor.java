package ru.mail.polis.service.saloed.request.processor;

import one.nio.http.Request;
import one.nio.http.Response;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;

import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.service.saloed.request.RequestUtils;
import ru.mail.polis.service.saloed.request.processor.entity.Arguments;
import ru.mail.polis.service.saloed.request.processor.entity.DeleteEntityRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.entity.GetEntityRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.entity.MaybeRecordWithTimestamp;
import ru.mail.polis.service.saloed.request.processor.entity.UpsertEntityRequestProcessor;

public abstract class EntityRequestProcessor {

    public final DAOWithTimestamp dao;
    public static final String REQUEST_PATH = "/v0/entity";

    public EntityRequestProcessor(final DAOWithTimestamp dao) {
        this.dao = dao;
    }

    /**
     * Returns processor for entity requests according to HTTP method.
     *
     * @param method HTTP method
     * @param dao    to access data
     * @return entity processor
     */
    public static EntityRequestProcessor forHttpMethod(
            final int method,
            final DAOWithTimestamp dao) {
        switch (method) {
            case Request.METHOD_GET:
                return new GetEntityRequestProcessor(dao);
            case Request.METHOD_PUT:
                return new UpsertEntityRequestProcessor(dao);
            case Request.METHOD_DELETE:
                return new DeleteEntityRequestProcessor(dao);
            default:
                throw new IllegalArgumentException(
                        "Processor for method is unavailable: " + method);
        }
    }

    /**
     * Make some preprocessing on HTTP request.
     *
     * @param request   HTTP request
     * @param arguments of entity operation
     * @return modified request
     */
    public HttpRequest.Builder preprocessRemote(final HttpRequest.Builder request, final Arguments arguments) {
        final var processed = RequestUtils.setRequestFromService(request);
        return RequestUtils.setRequestTimestamp(processed, arguments.getTimestamp());
    }

    /**
     * Perform entity operation on a local node.
     *
     * @param arguments of entity operation
     * @return operation result
     */
    public abstract Optional<MaybeRecordWithTimestamp> processLocal(final Arguments arguments);

    /**
     * Retrieve operation result from remote node response.
     *
     * @param response  from remote node
     * @param arguments of entity operation
     * @return operation result
     */
    public abstract Optional<MaybeRecordWithTimestamp> obtainRemoteResult(final HttpResponse<byte[]> response,
                                                                          final Arguments arguments);

    /**
     * Make an HTTP response for user, based on operation results.
     *
     * @param data      operation results
     * @param arguments of entity operation
     * @return HTTP response
     */
    public abstract Response makeResponseForUser(final List<MaybeRecordWithTimestamp> data,
                                                 final Arguments arguments);

    /**
     * Make an HTTP response for service, based on operation result.
     *
     * @param data      operation result
     * @param arguments of entity operation
     * @return HTTP response
     */
    public abstract Response makeResponseForService(final MaybeRecordWithTimestamp data,
                                                    final Arguments arguments);

}
