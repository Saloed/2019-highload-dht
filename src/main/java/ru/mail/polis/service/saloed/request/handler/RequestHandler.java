package ru.mail.polis.service.saloed.request.handler;

import java.io.IOException;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ru.mail.polis.service.saloed.ServiceMetrics;

public final class RequestHandler implements Runnable {

    private static final Log log = LogFactory.getLog(RequestHandler.class);

    private final ServiceMetrics metrics;
    private final HttpSession session;
    private final Runnable runnable;

    /**
     * HTTP request handler, which supports rejection handling.
     *
     * @param metrics  of service
     * @param session  HTTP session
     * @param runnable request handling code
     */
    public RequestHandler(
        final ServiceMetrics metrics,
        final HttpSession session,
        final Runnable runnable) {
        this.metrics = metrics;
        this.session = session;
        this.runnable = runnable;
    }

    @Override
    public void run() {
        runnable.run();
    }

    void handleRejectedRequest() {
        metrics.errorResponse();
        try {
            session.sendError(Response.SERVICE_UNAVAILABLE, "Rejected");
        } catch (IOException exception) {
            log.error("Error during send response", exception);
        }
    }
}
