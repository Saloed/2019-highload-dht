package ru.mail.polis.service.saloed;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class ServiceMetrics implements Closeable {

    private static final Log log = LogFactory.getLog(ServiceMetrics.class);
    private static final String FORMAT = "| %-10.10s | %-10.10s | %-10.10s | %-10.10s |\n";
    private static final Duration PERIOD = Duration.ofSeconds(10);

    private final AtomicInteger request = new AtomicInteger(0);
    private final AtomicInteger userRequest = new AtomicInteger(0);
    private final AtomicInteger serviceRequest = new AtomicInteger(0);
    private final AtomicInteger successResponse = new AtomicInteger(0);
    private final AtomicInteger successUserResponse = new AtomicInteger(0);
    private final AtomicInteger successServiceResponse = new AtomicInteger(0);
    private final AtomicInteger errorResponse = new AtomicInteger(0);
    private final AtomicInteger errorUserResponse = new AtomicInteger(0);
    private final AtomicInteger errorServiceResponse = new AtomicInteger(0);
    private final Map<Integer, AtomicInteger> responseStatuses = new ConcurrentHashMap<>(10);
    private final Timer timer;
    private final ClusterNodeRouter nodeRouter;
    private final ServiceImpl server;

    ServiceMetrics(final ClusterNodeRouter nodeRouter, final ServiceImpl server) {
        this.nodeRouter = nodeRouter;
        this.server = server;
        timer = new Timer();
        initializeInfoLoop(timer);
        reset();
    }

    void request() {
        request.incrementAndGet();
    }

    void userRequest() {
        userRequest.incrementAndGet();
    }

    void serviceRequest() {
        serviceRequest.incrementAndGet();
    }

    void request(final boolean isServiceRequest) {
        if (isServiceRequest) {
            serviceRequest();
        } else {
            userRequest();
        }
    }

    void successResponse() {
        successResponse.incrementAndGet();
    }

    void successResponse(final boolean isServiceRequest) {
        if (isServiceRequest) {
            successServiceResponse();
        } else {
            successUserResponse();
        }
    }

    void successUserResponse() {
        successResponse.incrementAndGet();
        successUserResponse.incrementAndGet();
    }

    void successServiceResponse() {
        successResponse.incrementAndGet();
        successServiceResponse.incrementAndGet();
    }

    public void errorResponse() {
        errorResponse.incrementAndGet();
    }

    void errorUserResponse() {
        errorResponse.incrementAndGet();
        errorUserResponse.incrementAndGet();
    }

    void errorServiceResponse() {
        errorResponse.incrementAndGet();
        errorServiceResponse.incrementAndGet();
    }

    void responseWithStatus(final int status) {
        final var counter = responseStatuses.get(status);
        if (counter != null) {
            counter.incrementAndGet();
            return;
        }
        responseStatuses.put(status, new AtomicInteger(1));
    }

    private String makeHeader(final String name) {
        return String.format(FORMAT, name, "total", "user", "service");
    }

    private String makeData(final AtomicInteger total, final AtomicInteger user,
        final AtomicInteger service) {
        return String.format(FORMAT, "", total.get(), user.get(), service.get());
    }

    private String nodeRouterStats() {
        final var workers = nodeRouter.getWorkers();
        return "Node router workers: "
            + workers.toString()
            + '\n';
    }

    private String serverStats() {
        return "Server: "
            + "Processed "
            + server.getRequestsProcessed()
            + " Rejected "
            + server.getRequestsRejected()
            + " Avg queue "
            + server.getQueueAvgLength()
            + "\n";
    }

    private String getInfoString() {
        return "\n"
            + makeHeader("Requests")
            + makeData(request, userRequest, serviceRequest)
            + makeHeader("Success")
            + makeData(successResponse, successUserResponse, successServiceResponse)
            + makeHeader("Error")
            + makeData(errorResponse, errorUserResponse, errorServiceResponse)
            + responseStatuses.toString()
            + '\n'
            + nodeRouterStats()
            + serverStats()
            + '\n';
    }

    private void reset() {
        request.set(0);
        userRequest.set(0);
        serviceRequest.set(0);
        successResponse.set(0);
        successUserResponse.set(0);
        successServiceResponse.set(0);
        errorResponse.set(0);
        errorUserResponse.set(0);
        errorServiceResponse.set(0);
        responseStatuses.clear();
    }

    private void initializeInfoLoop(final Timer timer) {
        final var task = new InfoTask(this);
        timer.scheduleAtFixedRate(task, 0, TimeUnit.MILLISECONDS.convert(PERIOD));
    }

    @Override
    public void close() {
        timer.cancel();
    }

    private final static class InfoTask extends TimerTask {

        private final ServiceMetrics metrics;

        private InfoTask(final ServiceMetrics metrics) {
            this.metrics = metrics;
        }

        @Override
        public void run() {
            log.info(metrics.getInfoString());
        }
    }

}
