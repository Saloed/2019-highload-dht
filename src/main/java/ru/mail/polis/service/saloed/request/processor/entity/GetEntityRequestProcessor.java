package ru.mail.polis.service.saloed.request.processor.entity;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import one.nio.http.Response;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.service.saloed.request.ResponseUtils;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;

public class GetEntityRequestProcessor extends
    EntityRequestProcessor {

    public GetEntityRequestProcessor(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal(
        final Arguments arguments) {
        try {
            final var record = dao.getRecord(arguments.getKey());
            final var maybe = new MaybeRecordWithTimestamp(record);
            return Optional.of(maybe);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> obtainRemoteResult(
        final Response response, final Arguments arguments) {
        if (response.getStatus() != 200) {
            return Optional.empty();
        }
        final var record = RecordWithTimestamp.fromBytes(response.getBody());
        return Optional.of(new MaybeRecordWithTimestamp(record));
    }

    @Override
    public Response makeResponseForUser(
        final List<MaybeRecordWithTimestamp> data, final Arguments arguments) {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.notEnoughReplicas();
        }
        final var notEmptyRecords = data.stream()
            .map(MaybeRecordWithTimestamp::getRecord)
            .filter(it -> !it.isEmpty())
            .collect(Collectors.toList());
        if (notEmptyRecords.isEmpty()) {
            return ResponseUtils.notFound();
        }
        final var lastRecord = notEmptyRecords.stream()
            .max(Comparator.comparingLong(RecordWithTimestamp::getTimestamp))
            .get();
        if (lastRecord.isValue()) {
            final var valueArray = ByteBufferUtils.toArray(lastRecord.getValue());
            return new Response(Response.OK, valueArray);
        }
        return ResponseUtils.notFound();
    }

    @Override
    public Response makeResponseForService(
        final MaybeRecordWithTimestamp data, final Arguments arguments) {
        return new Response(Response.OK, data.getRecord().toRawBytes());
    }

}
