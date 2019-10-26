package ru.mail.polis.service.saloed.request.processor;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.service.saloed.request.processor.entity.*;
import ru.mail.polis.service.saloed.request.RequestUtils;

import java.util.List;
import java.util.Optional;

public abstract class EntityRequestProcessor {

    public final DAOWithTimestamp dao;

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

    public Request preprocessRemote(Request request, Arguments arguments) {
        RequestUtils.setRequestFromService(request);
        RequestUtils.setRequestTimestamp(request, arguments.getTimestamp());
        return request;
    }

    public abstract Optional<MaybeRecordWithTimestamp> processLocal(final Arguments arguments);

    public abstract Optional<MaybeRecordWithTimestamp> obtainRemoteResult(final Response response, final Arguments arguments);

    public abstract Response makeResponseForUser(final List<MaybeRecordWithTimestamp> data, final Arguments arguments);

    public abstract Response makeResponseForService(final MaybeRecordWithTimestamp data, final Arguments arguments);


}
