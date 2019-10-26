package ru.mail.polis.service.saloed;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.dao.RecordWithTimestamp;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

abstract class EntityRequestProcessor {

    final DAOWithTimestamp dao;

    EntityRequestProcessor(final DAOWithTimestamp dao) {
        this.dao = dao;
    }

    /**
     * Returns processor for entity requests according to HTTP method.
     *
     * @param method HTTP method
     * @param dao    to access data
     * @return entity processor
     */
    static EntityRequestProcessor forHttpMethod(
            final int method,
            final DAOWithTimestamp dao) {
        switch (method) {
            case Request.METHOD_GET:
                return new GetEntityRequestProcessor(dao);
            case Request.METHOD_PUT:
                return new UpsertEntityRequestProcessor(dao);
            case Request.METHOD_DELETE:
                return new DeleteEntityRequestProcessor(dao);
            default:
                throw new IllegalArgumentException(
                        "Processor for method is unavailable: " + method);
        }
    }

    public Request preprocessRemote(Request request, Arguments arguments) {
        RequestUtils.setRequestFromService(request);
        RequestUtils.setRequestTimestamp(request, arguments.getTimestamp());
        return request;
    }

    public abstract Optional<MaybeRecordWithTimestamp> processLocal(final Arguments arguments);

    public abstract Optional<MaybeRecordWithTimestamp> obtainRemoteResult(final Response response, final Arguments arguments);

    public abstract Response makeResponseForUser(final List<MaybeRecordWithTimestamp> data, final Arguments arguments);

    public abstract Response makeResponseForService(final MaybeRecordWithTimestamp data, final Arguments arguments);

    public static class Arguments {

        private final ByteBuffer key;
        private final long timestamp;
        private final int replicasAck;
        private final int replicasFrom;
        private boolean serviceRequest;

        Arguments(
                final ByteBuffer key,
                final boolean serviceRequest,
                final long timestamp,
                final int replicasAck,
                final int replicasFrom) {
            this.key = key;
            this.serviceRequest = serviceRequest;
            this.timestamp = timestamp;
            this.replicasAck = replicasAck;
            this.replicasFrom = replicasFrom;
        }

        public ByteBuffer getKey() {
            return key;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public int getReplicasAck() {
            return replicasAck;
        }

        public int getReplicasFrom() {
            return replicasFrom;
        }

        public boolean isServiceRequest() {
            return serviceRequest;
        }
    }


    static class MaybeRecordWithTimestamp {

        static final MaybeRecordWithTimestamp EMPTY = new MaybeRecordWithTimestamp(null);
        private final RecordWithTimestamp record;

        MaybeRecordWithTimestamp(final RecordWithTimestamp record) {
            this.record = record;
        }

        public RecordWithTimestamp getRecord() {
            if (record == null) {
                throw new IllegalStateException("Record is not present");
            }
            return record;
        }
    }

}
