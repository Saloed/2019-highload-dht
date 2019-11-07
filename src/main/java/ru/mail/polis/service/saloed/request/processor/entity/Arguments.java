package ru.mail.polis.service.saloed.request.processor.entity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import one.nio.http.Request;
import ru.mail.polis.service.saloed.request.RequestUtils;

public class Arguments {

    private static final Pattern REPLICAS_PATTERN = Pattern.compile("(\\d+)/(\\d+)");

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

    private static int quorum(final int number) {
        return number / 2 + 1;
    }

    /**
     * Retrieve arguments from entity HTTP request.
     *
     * @param id         of entity
     * @param replicas   for request
     * @param nodesCount number of nodes in the cluster
     * @param request    HTTP request
     * @return parsed arguments or null if request parameters are incorrect
     */
    public static Arguments fromHttpRequest(
        final String id,
        final String replicas,
        final int nodesCount,
        final Request request) {
        if (id == null || id.isEmpty()) {
            return null;
        }
        int replicasAck;
        int replicasFrom;
        final var matcher = REPLICAS_PATTERN.matcher(replicas == null ? "" : replicas);
        if (matcher.find()) {
            replicasAck = Integer.parseInt(matcher.group(1));
            replicasFrom = Integer.parseInt(matcher.group(2));
        } else {
            replicasAck = quorum(nodesCount);
            replicasFrom = nodesCount;
        }
        if (replicasFrom > nodesCount || replicasAck > replicasFrom
            || replicasAck < 1) {
            return null;
        }
        final var timestamp = RequestUtils.getRequestTimestamp(request);
        final var isServiceRequest = RequestUtils.isRequestFromService(request);
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        if (request.getMethod() == Request.METHOD_PUT) {
            final var body = ByteBuffer.wrap(request.getBody());
            return new UpsertArguments(key, id, body, isServiceRequest, timestamp, replicasAck,
                replicasFrom);
        }
        return new Arguments(key, id, isServiceRequest, timestamp, replicasAck, replicasFrom);
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
