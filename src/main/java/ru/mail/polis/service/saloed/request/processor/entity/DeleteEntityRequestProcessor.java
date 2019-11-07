package ru.mail.polis.service.saloed.request.processor.entity;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.util.Collection;
import java.util.Optional;
import one.nio.http.Response;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.service.saloed.request.ResponseUtils;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;

public class DeleteEntityRequestProcessor extends EntityRequestProcessor {

    public DeleteEntityRequestProcessor(final DAOWithTimestamp dao, final Arguments arguments) {
        super(dao, arguments);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal() {
        final var record = RecordWithTimestamp.tombstone(arguments.getTimestamp());
        try {
            dao.upsertRecord(arguments.getKey(), record);
            return Optional.of(MaybeRecordWithTimestamp.EMPTY);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public BodySubscriber<Optional<MaybeRecordWithTimestamp>> obtainRemoteResult(
        final ResponseInfo response) {
        return HttpResponse.BodySubscribers.replacing(
            response.statusCode() == 202
                ? Optional.of(MaybeRecordWithTimestamp.EMPTY)
                : Optional.empty());
    }

    @Override
    public Response makeResponseForUser(
        final Collection<MaybeRecordWithTimestamp> data) {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.notEnoughReplicas();
        }
        return ResponseUtils.accepted();
    }

    @Override
    public Response makeResponseForService(
        final MaybeRecordWithTimestamp data) {
        return ResponseUtils.accepted();
    }

    @Override
    public HttpRequest.Builder preprocessRemote(final HttpRequest.Builder request) {
        return super.preprocessRemote(request).DELETE();
    }
}
