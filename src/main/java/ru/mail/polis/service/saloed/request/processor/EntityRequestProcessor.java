package ru.mail.polis.service.saloed.request.processor;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.util.Collection;
import java.util.Optional;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.service.saloed.request.RequestUtils;
import ru.mail.polis.service.saloed.request.processor.entity.Arguments;
import ru.mail.polis.service.saloed.request.processor.entity.DeleteEntityRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.entity.GetEntityRequestProcessor;
import ru.mail.polis.service.saloed.request.processor.entity.MaybeRecordWithTimestamp;
import ru.mail.polis.service.saloed.request.processor.entity.UpsertArguments;
import ru.mail.polis.service.saloed.request.processor.entity.UpsertEntityRequestProcessor;

public abstract class EntityRequestProcessor {

    public static final String REQUEST_PATH = "/v0/entity";
    public final DAOWithTimestamp dao;
    public final Arguments arguments;

    public EntityRequestProcessor(final DAOWithTimestamp dao, final Arguments arguments) {
        this.dao = dao;
        this.arguments = arguments;
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
        final DAOWithTimestamp dao,
        final Arguments arguments) {
        switch (method) {
            case Request.METHOD_GET:
                return new GetEntityRequestProcessor(dao, arguments);
            case Request.METHOD_PUT:
                if (!(arguments instanceof UpsertArguments)) {
                    throw new IllegalArgumentException("Upsert arguments expected");
                }
                return new UpsertEntityRequestProcessor(dao, (UpsertArguments) arguments);
            case Request.METHOD_DELETE:
                return new DeleteEntityRequestProcessor(dao, arguments);
            default:
                return null;
        }
    }

    /**
     * Make some preprocessing on HTTP request.
     *
     * @param request HTTP request
     * @return modified request
     */
    public HttpRequest.Builder preprocessRemote(final HttpRequest.Builder request) {
        final var processed = RequestUtils.setRequestFromService(request);
        return RequestUtils.setRequestTimestamp(processed, arguments.getTimestamp());
    }

    /**
     * Perform entity operation on a local node.
     *
     * @return operation result
     */
    public abstract Optional<MaybeRecordWithTimestamp> processLocal();

    /**
     * Retrieve operation result from remote node response.
     *
     * @param response HTTP response
     * @return operation result
     */
    public abstract BodySubscriber<Optional<MaybeRecordWithTimestamp>> obtainRemoteResult(
        final ResponseInfo response);

    /**
     * Make an HTTP response for user, based on operation results.
     *
     * @param data operation results
     * @return HTTP response
     */
    public abstract Response makeResponseForUser(final Collection<MaybeRecordWithTimestamp> data);

    /**
     * Make an HTTP response for service, based on operation result.
     *
     * @param data operation result
     * @return HTTP response
     */
    public abstract Response makeResponseForService(final MaybeRecordWithTimestamp data);

}
