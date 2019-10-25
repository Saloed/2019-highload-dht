package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.nio.ByteBuffer;
import one.nio.http.Request;
import ru.mail.polis.dao.DAOWithTimestamp;
import ru.mail.polis.service.saloed.EntityClusterTask.Arguments;

abstract class EntityClusterTask<R, D extends Arguments> implements ClusterTask<R, D> {

    final DAOWithTimestamp dao;

    EntityClusterTask(final DAOWithTimestamp dao) {
        this.dao = dao;
    }

    /**
     * Returns processor for entity requests according to HTTP method.
     *
     * @param method HTTP method
     * @param dao    to access data
     * @return entity processor
     */
    static EntityClusterTask forHttpMethod(
        final int method,
        final DAOWithTimestamp dao) {
        switch (method) {
            case Request.METHOD_GET:
                return new GetEntityClusterTask(dao);
            case Request.METHOD_PUT:
                return new UpsertEntityClusterTask(dao);
            case Request.METHOD_DELETE:
                return new DeleteEntityClusterTask(dao);
            default:
                throw new IllegalArgumentException(
                    "Processor for method is unavailable: " + method);
        }
    }

    @Override
    public Request preprocessRemote(Request request, D arguments)
        throws IOException {
        RequestUtils.setRequestFromService(request);
        RequestUtils.setRequestTimestamp(request, arguments.getTimestamp());
        return request;
    }

    @Override
    public boolean isLocalTaskForService(D arguments) {
        return arguments.isServiceRequest();
    }

    public static class Arguments implements ClusterTaskArguments {

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

}
