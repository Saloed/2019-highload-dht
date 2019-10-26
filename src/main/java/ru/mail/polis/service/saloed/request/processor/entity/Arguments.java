package ru.mail.polis.service.saloed.request.processor.entity;

import java.nio.ByteBuffer;

public class Arguments {

    private final ByteBuffer key;
    private final long timestamp;
    private final int replicasAck;
    private final int replicasFrom;
    private boolean serviceRequest;

    public Arguments(
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
