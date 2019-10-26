package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import one.nio.http.Response;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

public class GetEntityRequestProcessor extends
        EntityRequestProcessor {

    GetEntityRequestProcessor(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal(Arguments arguments) {
        try {
            final var record = dao.getRecord(arguments.getKey());
            final var maybe = new MaybeRecordWithTimestamp(record);
            return Optional.of(maybe);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> obtainRemoteResult(Response response, Arguments arguments) {
        if (response.getStatus() != 200) {
            return Optional.empty();
        }
        final var record = RecordWithTimestamp.fromBytes(response.getBody());
        return Optional.of(new MaybeRecordWithTimestamp(record));
    }

    @Override
    public Response makeResponseForUser(List<MaybeRecordWithTimestamp> data, Arguments arguments) {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.NOT_ENOUGH_REPLICAS;
        }
        final var notEmptyRecords = data.stream()
                .map(MaybeRecordWithTimestamp::getRecord)
                .filter(it -> !it.isEmpty())
                .collect(Collectors.toList());
        if (notEmptyRecords.isEmpty()) {
            return ResponseUtils.NOT_FOUND;
        }
        final var lastRecord = notEmptyRecords.stream()
                .max(Comparator.comparingLong(RecordWithTimestamp::getTimestamp))
                .get();
        if (lastRecord.isValue()) {
            final var valueArray = ByteBufferUtils.toArray(lastRecord.getValue());
            return new Response(Response.OK, valueArray);
        }
        return ResponseUtils.NOT_FOUND;
    }

    @Override
    public Response makeResponseForService(MaybeRecordWithTimestamp data, Arguments arguments) {
        return new Response(Response.OK, data.getRecord().toRawBytes());
    }

}
