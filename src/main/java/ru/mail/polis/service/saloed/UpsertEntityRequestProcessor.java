package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import one.nio.http.Response;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

public class UpsertEntityRequestProcessor extends
        EntityRequestProcessor {

    UpsertEntityRequestProcessor(final DAOWithTimestamp dao) {
        super(dao);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal(Arguments arguments){
        if(!(arguments instanceof UpsertArguments)){
            throw new IllegalArgumentException("Upsert arguments expected");
        }
        final var upsertArguments = (UpsertArguments) arguments;
        final var record = RecordWithTimestamp
                .fromValue(upsertArguments.getValue(), arguments.getTimestamp());
        try {
            dao.upsertRecord(arguments.getKey(), record);
        } catch (IOException ex){
            return Optional.empty();
        }
        return Optional.of(MaybeRecordWithTimestamp.EMPTY);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> obtainRemoteResult(Response response, Arguments arguments) {
        if (response.getStatus() == 201) {
            return Optional.of(MaybeRecordWithTimestamp.EMPTY);
        }
        return Optional.empty();
    }

    @Override
    public Response makeResponseForUser(List<MaybeRecordWithTimestamp> data, Arguments arguments) {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.NOT_ENOUGH_REPLICAS;
        }
        return ResponseUtils.CREATED;
    }

    @Override
    public Response makeResponseForService(MaybeRecordWithTimestamp data, Arguments arguments) {
        return ResponseUtils.CREATED;
    }

    public static class UpsertArguments extends EntityRequestProcessor.Arguments {

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
