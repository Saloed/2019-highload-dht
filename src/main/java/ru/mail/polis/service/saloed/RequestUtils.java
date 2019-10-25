package ru.mail.polis.service.saloed;

import one.nio.http.Request;

public class RequestUtils {

    private static final String TIMESTAMP_HEADER = "X-Service-Timestamp:";
    private static final String SERVICE_REQUEST_HEADER = "X-Service-Request:";

    private RequestUtils() {
    }

    public static void setRequestTimestamp(final Request request, final long timestamp) {
        request.addHeader(TIMESTAMP_HEADER + timestamp);
    }

    public static void setRequestFromService(final Request request) {
        request.addHeader(SERVICE_REQUEST_HEADER + "true");
    }


    public static boolean isRequestFromService(final Request request) {
        final var header = request.getHeader(SERVICE_REQUEST_HEADER);
        if (header == null) {
            return false;
        }
        return Boolean.parseBoolean(header);
    }

    public static long getRequestTimestamp(final Request request) {
        final var header = request.getHeader(TIMESTAMP_HEADER);
        if (header == null) {
            return System.currentTimeMillis();
        }
        return Long.parseLong(header);
    }

}
