package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

public class DeleteEntityClusterTask extends
    EntityClusterTask<SuccessResult, EntityClusterTask.Arguments> {

    DeleteEntityClusterTask(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public SuccessResult processLocal(Arguments arguments) throws IOException {
        final var record = RecordWithTimestamp.tombstone(arguments.getTimestamp());
        dao.upsertRecord(arguments.getKey(), record);
        return SuccessResult.INSTANCE;
    }

    @Override
    public Optional<SuccessResult> obtainRemoteResult(Response response, Arguments arguments)
        throws IOException {
        if (response.getStatus() == 202) {
            return Optional.of(SuccessResult.INSTANCE);
        }
        return Optional.empty();
    }

    @Override
    public Response makeResponseForUser(List<SuccessResult> data, Arguments arguments)
        throws IOException {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.NOT_ENOUGH_REPLICAS;
        }
        return ResponseUtils.ACCEPTED;
    }

    @Override
    public Response makeResponseForService(SuccessResult data, Arguments arguments)
        throws IOException {
        return ResponseUtils.ACCEPTED;
    }


}
