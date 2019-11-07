package ru.mail.polis.service.saloed.request.processor.entities;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class Arguments {

    private final String startStr;
    private final String endStr;
    private final ByteBuffer start;
    private final ByteBuffer end;
    private final boolean isServiceRequest;

    private Arguments(
        final String startStr,
        final String endStr,
        final ByteBuffer start,
        final ByteBuffer end,
        final boolean isServiceRequest) {
        this.startStr = startStr;
        this.endStr = endStr;
        this.start = start;
        this.end = end;
        this.isServiceRequest = isServiceRequest;
    }

    /**
     * Parse an arguments of entities request.
     *
     * @param start            of entities range
     * @param end              of entities range
     * @param isServiceRequest request from user or service
     * @return arguments or null, if input is incorrect
     */
    public static Arguments parse(
        final String start,
        final String end,
        final boolean isServiceRequest) {
        if (start == null || start.isEmpty()) {
            return null;
        }
        final var startBytes = ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));
        ByteBuffer endBytes = null;
        if (end != null && !end.isEmpty()) {
            endBytes = ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8));
        }
        return new Arguments(start, end, startBytes, endBytes, isServiceRequest);
    }

    public boolean isServiceRequest() {
        return isServiceRequest;
    }

    public boolean hasEnd() {
        return endStr != null && end != null;
    }

    public String getStartStr() {
        return startStr;
    }

    public ByteBuffer getStart() {
        return start;
    }

    public ByteBuffer getEnd() {
        return end;
    }

    public String getEndStr() {
        return endStr;
    }
}
