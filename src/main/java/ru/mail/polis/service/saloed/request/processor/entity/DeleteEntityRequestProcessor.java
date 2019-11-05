package ru.mail.polis.service.saloed.request.processor.entity;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;

import one.nio.http.Response;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.service.saloed.request.ResponseUtils;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;

public class DeleteEntityRequestProcessor extends EntityRequestProcessor {

    public DeleteEntityRequestProcessor(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal(final Arguments arguments) {
        final var record = RecordWithTimestamp.tombstone(arguments.getTimestamp());
        try {
            dao.upsertRecord(arguments.getKey(), record);
            return Optional.of(MaybeRecordWithTimestamp.EMPTY);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> obtainRemoteResult(
            final HttpResponse<byte[]> response, final Arguments arguments) {
        if (response.statusCode() == 202) {
            return Optional.of(MaybeRecordWithTimestamp.EMPTY);
        }
        return Optional.empty();
    }

    @Override
    public Response makeResponseForUser(
        final List<MaybeRecordWithTimestamp> data, final Arguments arguments) {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.notEnoughReplicas();
        }
        return ResponseUtils.accepted();
    }

    @Override
    public Response makeResponseForService(
        final MaybeRecordWithTimestamp data, final Arguments arguments) {
        return ResponseUtils.accepted();
    }

    @Override
    public HttpRequest.Builder preprocessRemote(final HttpRequest.Builder request, final Arguments arguments) {
        return super.preprocessRemote(request, arguments).DELETE();
    }
}
