package ru.mail.polis.service.saloed.request.processor.entity;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import one.nio.http.Response;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.service.saloed.request.ResponseUtils;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;

public class UpsertEntityRequestProcessor extends
    EntityRequestProcessor {

    public UpsertEntityRequestProcessor(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal(
        final Arguments arguments) {
        if (!(arguments instanceof UpsertArguments)) {
            throw new IllegalArgumentException("Upsert arguments expected");
        }
        final var upsertArguments = (UpsertArguments) arguments;
        final var record = RecordWithTimestamp
            .fromValue(upsertArguments.getValue(), arguments.getTimestamp());
        try {
            dao.upsertRecord(arguments.getKey(), record);
        } catch (IOException ex) {
            return Optional.empty();
        }
        return Optional.of(MaybeRecordWithTimestamp.EMPTY);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> obtainRemoteResult(
        final HttpResponse<byte[]> response, final Arguments arguments) {
        if (response.statusCode() == 201) {
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
        return ResponseUtils.created();
    }

    @Override
    public Response makeResponseForService(
        final MaybeRecordWithTimestamp data, final Arguments arguments) {
        return ResponseUtils.created();
    }

    @Override
    public HttpRequest.Builder preprocessRemote(final HttpRequest.Builder request,
        final Arguments arguments) {
        if (!(arguments instanceof UpsertArguments)) {
            throw new IllegalArgumentException("Upsert arguments expected");
        }
        final var upsertArguments = (UpsertArguments) arguments;
        final var byteArray = ByteBufferUtils.toArray(upsertArguments.getValue());
        final var publisher = HttpRequest.BodyPublishers.ofByteArray(byteArray);
        return super.preprocessRemote(request, arguments).PUT(publisher);
    }
}
