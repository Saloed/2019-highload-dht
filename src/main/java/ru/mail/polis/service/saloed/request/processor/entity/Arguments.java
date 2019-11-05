package ru.mail.polis.service.saloed.request.processor.entity;

import java.nio.ByteBuffer;

public class Arguments {

    private final ByteBuffer key;
    private final long timestamp;
    private final int replicasAck;
    private final int replicasFrom;
    private final String keyString;
    private final boolean serviceRequest;

    /**
     * Arguments of entity request.
     *
     * @param key            of entity
     * @param keyString      key as string
     * @param serviceRequest is request from service or not
     * @param timestamp      of request
     * @param replicasAck    required replicas acknowledge count
     * @param replicasFrom   replicas count
     */
    public Arguments(
            final ByteBuffer key,
            final String keyString,
            final boolean serviceRequest,
            final long timestamp,
            final int replicasAck,
            final int replicasFrom) {
        this.key = key;
        this.keyString = keyString;
        this.serviceRequest = serviceRequest;
        this.timestamp = timestamp;
        this.replicasAck = replicasAck;
        this.replicasFrom = replicasFrom;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public String getKeyString() {
        return keyString;
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
