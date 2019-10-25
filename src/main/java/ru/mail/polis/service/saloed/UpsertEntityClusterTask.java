package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

public class UpsertEntityClusterTask extends
    EntityClusterTask<SuccessResult, UpsertEntityClusterTask.UpsertArguments> {

    UpsertEntityClusterTask(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public SuccessResult processLocal(UpsertArguments arguments)
        throws IOException {
        final var record = RecordWithTimestamp
            .fromValue(arguments.getValue(), arguments.getTimestamp());
        dao.upsertRecord(arguments.getKey(), record);
        return SuccessResult.INSTANCE;
    }

    @Override
    public Optional<SuccessResult> obtainRemoteResult(Response response,
        UpsertArguments arguments) throws IOException {
        if (response.getStatus() == 201) {
            return Optional.of(SuccessResult.INSTANCE);
        }
        return Optional.empty();
    }

    @Override
    public Response makeResponseForUser(List<SuccessResult> data, UpsertArguments arguments)
        throws IOException {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.NOT_ENOUGH_REPLICAS;
        }
        return ResponseUtils.CREATED;
    }

    @Override
    public Response makeResponseForService(SuccessResult data, UpsertArguments arguments)
        throws IOException {
        return ResponseUtils.CREATED;
    }


    public static class UpsertArguments extends EntityClusterTask.Arguments {

        private final ByteBuffer value;

        UpsertArguments(
            final ByteBuffer key,
            final ByteBuffer value,
            final boolean serviceRequest,
            final long timestamp,
            final int replicasAck,
            final int replicasFrom) {
            super(key, serviceRequest, timestamp, replicasAck, replicasFrom);
            this.value = value;
        }

        public ByteBuffer getValue() {
            return value;
        }
    }

}
