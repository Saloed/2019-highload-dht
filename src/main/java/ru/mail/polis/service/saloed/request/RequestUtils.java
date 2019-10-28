package ru.mail.polis.service.saloed.request;

import one.nio.http.Request;

public final class RequestUtils {

    private static final String TIMESTAMP_HEADER = "X-Service-Timestamp:";
    private static final String SERVICE_REQUEST_HEADER = "X-Service-Request:";

    private RequestUtils() {
    }

    /**
     * Set request timestamp header.
     *
     * @param request   HTTP request
     * @param timestamp is a timestamp
     */
    public static void setRequestTimestamp(final Request request, final long timestamp) {
        request.addHeader(TIMESTAMP_HEADER + timestamp);
    }

    /**
     * Set request header, which identifies request from service.
     *
     * @param request HTTP request
     */
    public static void setRequestFromService(final Request request) {
        request.addHeader(SERVICE_REQUEST_HEADER + "true");
    }


    /**
     * Check request header, which identifies request from service.
     *
     * @param request HTTP request
     * @return result of check
     */
    public static boolean isRequestFromService(final Request request) {
        final var header = request.getHeader(SERVICE_REQUEST_HEADER);
        if (header == null) {
            return false;
        }
        return Boolean.parseBoolean(header);
    }

    /**
     * Retrieve timestamp of request. If request doesn't contains corresponding header, returns
     * current timestamp.
     *
     * @param request HTTP request
     * @return timestamp
     */
    public static long getRequestTimestamp(final Request request) {
        final var header = request.getHeader(TIMESTAMP_HEADER);
        if (header == null) {
            return System.currentTimeMillis();
        }
        return Long.parseLong(header);
    }

}
