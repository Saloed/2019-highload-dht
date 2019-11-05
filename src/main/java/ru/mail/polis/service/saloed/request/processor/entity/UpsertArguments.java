package ru.mail.polis.service.saloed.request.processor.entity;

import java.nio.ByteBuffer;

public class UpsertArguments extends Arguments {

    private final ByteBuffer value;

    public UpsertArguments(
            final ByteBuffer key,
            final String keyStr,
            final ByteBuffer value,
            final boolean serviceRequest,
            final long timestamp,
            final int replicasAck,
            final int replicasFrom) {
        super(key, keyStr, serviceRequest, timestamp, replicasAck, replicasFrom);
        this.value = value;
    }

    public ByteBuffer getValue() {
        return value;
    }
}
