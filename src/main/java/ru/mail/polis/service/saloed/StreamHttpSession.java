package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ru.mail.polis.service.saloed.payload.Payload;

public final class StreamHttpSession extends HttpSession implements Flow.Subscriber<Payload> {

    private static final Log log = LogFactory.getLog(StreamHttpSession.class);

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(StandardCharsets.UTF_8);
    private final Queue<Payload> streamQueue = new ConcurrentLinkedQueue<>();
    private Subscription subscription;

    StreamHttpSession(final Socket socket, final HttpServer server) {
        super(socket, server);
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    private void writeRecord(final Payload record) throws IOException {
        final var payload = record.toRawBytes();
        final var payloadLength = payload.length;
        final var payloadLengthHex = Integer.toHexString(payloadLength);
        final var chunkLength =
            payloadLengthHex.length() + CRLF.length + payloadLength + CRLF.length;

        final var chunk = new byte[chunkLength];
        final var chunkBuffer = ByteBuffer.wrap(chunk);
        chunkBuffer.put(payloadLengthHex.getBytes(StandardCharsets.UTF_8));
        chunkBuffer.put(CRLF);
        chunkBuffer.put(payload);
        chunkBuffer.put(CRLF);

        write(chunk, 0, chunk.length);
    }

    private boolean keepAlive() {
        final var connection = handling.getHeader("Connection: ");
        return handling.isHttp11()
            ? !"close".equalsIgnoreCase(connection)
            : "Keep-Alive".equalsIgnoreCase(connection);
    }

    private void handleStreamEnding() throws IOException {
        write(EMPTY, 0, EMPTY.length);
        server.incRequestsProcessed();

        if (!keepAlive()) {
            scheduleClose();
        }
        if ((handling = pipeline.pollFirst()) != null) {
            if (handling == FIN) {
                scheduleClose();
            } else {
                server.handleRequest(handling, this);
            }
        }
    }

    private void handleStreamStart() throws IOException {
        if (handling == null) {
            throw new IOExceptionLight("Out of order response");
        }
        final var response = new Response(Response.OK);
        response.addHeader(keepAlive() ? "Connection: Keep-Alive" : "Connection: close");
        response.addHeader("Transfer-Encoding: chunked");

        writeResponse(response, false);
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        this.subscription = subscription;
        try {
            handleStreamStart();
        } catch (IOException ex) {
            log.error("Unable to start stream", ex);
            handleUnexpectedStreamEnding();
            return;
        }
        subscription.request(1);
    }

    private void handleUnexpectedStreamEnding() {
        subscription.cancel();
        // todo
    }

    private void next() throws IOException {
        while (!streamQueue.isEmpty() && queueHead == null) {
            final var record = streamQueue.poll();
            writeRecord(record);
        }
        if (streamQueue.isEmpty()) {
            subscription.request(1);
        }
    }


    @Override
    public void onNext(final Payload item) {
        streamQueue.add(item);
        try {
            next();
        } catch (IOException ex) {
            log.error("Unable to write element to stream", ex);
            handleUnexpectedStreamEnding();
        }
    }

    @Override
    public void onError(final Throwable throwable) {
        log.error("Error while streaming", throwable);
        handleUnexpectedStreamEnding();
    }

    @Override
    public void onComplete() {
        try {
            handleStreamEnding();
        } catch (IOException ex) {
            log.error("Unable to end stream", ex);
            handleUnexpectedStreamEnding();
        }
    }
}
