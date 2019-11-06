package ru.mail.polis.service.saloed.request.handler;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public final class RejectedRequestHandler implements RejectedExecutionHandler {

    private final RejectedExecutionHandler defaultHandler;

    public RejectedRequestHandler(final RejectedExecutionHandler defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    @Override
    public void rejectedExecution(final Runnable runnable, final ThreadPoolExecutor executor) {
        if (!(runnable instanceof RequestHandler)) {
            defaultHandler.rejectedExecution(runnable, executor);
            return;
        }
        final var handler = (RequestHandler) runnable;
        handler.handleRejectedRequest();
    }
}
