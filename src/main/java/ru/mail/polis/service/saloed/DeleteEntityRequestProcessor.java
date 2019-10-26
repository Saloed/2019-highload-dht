package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import one.nio.http.Response;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

public class DeleteEntityRequestProcessor extends EntityRequestProcessor {

    DeleteEntityRequestProcessor(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal(Arguments arguments) {
        final var record = RecordWithTimestamp.tombstone(arguments.getTimestamp());
        try {
            dao.upsertRecord(arguments.getKey(), record);
            return Optional.of(MaybeRecordWithTimestamp.EMPTY);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> obtainRemoteResult(Response response, Arguments arguments) {
        if (response.getStatus() == 202) {
            return Optional.of(MaybeRecordWithTimestamp.EMPTY);
        }
        return Optional.empty();
    }

    @Override
    public Response makeResponseForUser(List<MaybeRecordWithTimestamp> data, Arguments arguments) {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.NOT_ENOUGH_REPLICAS;
        }
        return ResponseUtils.ACCEPTED;
    }

    @Override
    public Response makeResponseForService(MaybeRecordWithTimestamp data, Arguments arguments) {
        return ResponseUtils.ACCEPTED;
    }

}
