package ru.mail.polis.service.saloed;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ru.mail.polis.dao.IOExceptionLight;
import ru.mail.polis.service.saloed.payload.Payload;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public final class StreamHttpSession extends HttpSession {

    private static final Log log = LogFactory.getLog(StreamHttpSession.class);

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(StandardCharsets.UTF_8);

    private Iterator<Payload> recordIterator;

    StreamHttpSession(final Socket socket, final HttpServer server) {
        super(socket, server);
    }

    /**
     * Start streaming data from iterator as chunked data.
     *
     * @param recordIterator with data
     * @throws IOException if network errors occurred
     */
    public synchronized void stream(final Iterator<Payload> recordIterator) throws IOException {
        this.recordIterator = recordIterator;
        if (handling == null) {
            throw new IOExceptionLight("Out of order response");
        }
        final var response = new Response(Response.OK);
        response.addHeader(keepAlive() ? "Connection: Keep-Alive" : "Connection: close");
        response.addHeader("Transfer-Encoding: chunked");

        writeResponse(response, false);

        next();
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
        final var chunkLength = payloadLengthHex.length() + CRLF.length + payloadLength + CRLF.length;

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

    private synchronized void next() throws IOException {
        if (recordIterator == null) {
            throw new IllegalStateException("Iterator is missing");
        }
        while (recordIterator.hasNext() && queueHead == null) {
            final var record = recordIterator.next();
            writeRecord(record);
        }
        if (!recordIterator.hasNext()) {
            handleStreamEnding();
            if (recordIterator instanceof Closeable) {
                try {
                    ((Closeable) recordIterator).close();
                } catch (IOException exception) {
                    log.error("Exception while close iterator", exception);
                }
            }
            recordIterator = null;
        }
    }

}
