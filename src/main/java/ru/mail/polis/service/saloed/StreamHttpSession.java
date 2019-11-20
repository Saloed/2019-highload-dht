package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import ru.mail.polis.service.saloed.payload.Payload;

public final class StreamHttpSession extends HttpSession implements Flow.Subscriber<Payload> {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(StandardCharsets.UTF_8);
    private final Queue<Payload> streamQueue = new ConcurrentLinkedQueue<>();
    private Subscription subscription;
    private CompletableFuture<Void> streamCompletionHandler;

    StreamHttpSession(final Socket socket, final HttpServer server) {
        super(socket, server);
    }

    /**
     * Return future, which completes after stream finished or errored.
     *
     * @return future
     */
    public CompletableFuture<Void> whenStreamComplete() {
        if (streamCompletionHandler == null) {
            streamCompletionHandler = new CompletableFuture<>();
        }
        return streamCompletionHandler;
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        this.subscription = subscription;
        try {
            handleStreamStart();
            subscription.request(1);
        } catch (IOException ex) {
            handleUnexpectedStreamEnding(ex);
        }
    }

    @Override
    public void onNext(final Payload item) {
        streamQueue.add(item);
        try {
            next();
        } catch (IOException ex) {
            handleUnexpectedStreamEnding(ex);
        }
    }

    @Override
    public void onError(final Throwable throwable) {
        handleUnexpectedStreamEnding(throwable);
    }

    @Override
    public void onComplete() {
        try {
            handleStreamEnding();
        } catch (IOException ex) {
            handleUnexpectedStreamEnding(ex);
        }
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

        completionHandler(true, null);

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

    private void completionHandler(final boolean success, final Throwable ex) {
        if (streamCompletionHandler == null) {
            return;
        }
        if (success) {
            streamCompletionHandler.complete(null);
        } else {
            streamCompletionHandler.completeExceptionally(ex);
        }
        streamCompletionHandler = null;
    }

    private void handleUnexpectedStreamEnding(final Throwable ex) {
        subscription.cancel();
        subscription = null;
        close();
        streamQueue.clear();
        completionHandler(false, ex);
    }

    private void next() throws IOException {
        while (true) {
            if (queueHead != null) {
                break;
            }
            final var record = streamQueue.poll();
            if (record == null) {
                subscription.request(1);
                break;
            }
            writeRecord(record);
        }
    }

}
