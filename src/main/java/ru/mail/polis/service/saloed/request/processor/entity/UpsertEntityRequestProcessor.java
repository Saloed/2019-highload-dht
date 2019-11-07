package ru.mail.polis.service.saloed.request.processor.entity;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.util.Collection;
import java.util.Optional;
import one.nio.http.Response;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.service.saloed.request.ResponseUtils;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;

public class UpsertEntityRequestProcessor extends
    EntityRequestProcessor {

    private final UpsertArguments upsertArguments;

    public UpsertEntityRequestProcessor(final DAOWithTimestamp dao,
        final UpsertArguments arguments) {
        super(dao, arguments);
        this.upsertArguments = arguments;
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal() {
        final var record = RecordWithTimestamp
            .fromValue(upsertArguments.getValue(), upsertArguments.getTimestamp());
        try {
            dao.upsertRecord(upsertArguments.getKey(), record);
        } catch (IOException ex) {
            return Optional.empty();
        }
        return Optional.of(MaybeRecordWithTimestamp.EMPTY);
    }

    @Override
    public BodySubscriber<Optional<MaybeRecordWithTimestamp>> obtainRemoteResult(
        final ResponseInfo response) {
        return HttpResponse.BodySubscribers.replacing(
            response.statusCode() == 201
                ? Optional.of(MaybeRecordWithTimestamp.EMPTY)
                : Optional.empty());
    }

    @Override
    public Response makeResponseForUser(
        final Collection<MaybeRecordWithTimestamp> data) {
        if (data.size() < upsertArguments.getReplicasAck()) {
            return ResponseUtils.notEnoughReplicas();
        }
        return ResponseUtils.created();
    }

    @Override
    public Response makeResponseForService(
        final MaybeRecordWithTimestamp data) {
        return ResponseUtils.created();
    }

    @Override
    public HttpRequest.Builder preprocessRemote(final HttpRequest.Builder request) {
        final var byteArray = ByteBufferUtils.toArray(upsertArguments.getValue());
        final var publisher = HttpRequest.BodyPublishers.ofByteArray(byteArray);
        return super.preprocessRemote(request).PUT(publisher);
    }
}
