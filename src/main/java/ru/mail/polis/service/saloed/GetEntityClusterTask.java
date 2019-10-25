package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

public class GetEntityClusterTask extends
    EntityClusterTask<RecordWithTimestamp, EntityClusterTask.Arguments> {

    GetEntityClusterTask(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public RecordWithTimestamp processLocal(Arguments arguments)
        throws IOException {
        return dao.getRecord(arguments.getKey());
    }

    @Override
    public Optional<RecordWithTimestamp> obtainRemoteResult(Response response,
        Arguments arguments) throws IOException {
        if (response.getStatus() != 200) {
            return Optional.empty();
        }
        final var record = RecordWithTimestamp.fromBytes(response.getBody());
        return Optional.of(record);
    }

    @Override
    public Response makeResponseForUser(List<RecordWithTimestamp> data,
        Arguments arguments) throws IOException {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.NOT_ENOUGH_REPLICAS;
        }
        final var notEmptyRecords = data.stream()
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
    public Response makeResponseForService(RecordWithTimestamp data,
        Arguments arguments) throws IOException {
        return new Response(Response.OK, data.toRawBytes());
    }

}
