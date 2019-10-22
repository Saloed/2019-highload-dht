package ru.mail.polis.service.saloed;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.util.Utf8;

public class StreamHttpClient extends HttpClient {

    private static final long serialVersionUID = 6769829250639415682L;

    private static final int BUFFER_SIZE = 8000;

    public StreamHttpClient(final ConnectionString conn) {
        super(conn);
    }

    /**
     * Perform HTTP request, with expected response transfer encoding as chunked. Return iterator
     * over response chunks.
     *
     * @param request      HTTP request
     * @param chunkBuilder deserializer for retrieved chunks
     * @param <T>          type of chunk
     * @return iterator over chunks
     * @throws InterruptedException something bad happens
     * @throws PoolException        socket pool exception occurred
     * @throws IOException          if network error occurred
     * @throws HttpException        if http format exception occurred
     */
    public <T> StreamReader<T> invokeStream(final Request request,
        final ChunkBuilder<T> chunkBuilder)
        throws InterruptedException, PoolException, IOException, HttpException {
        byte[] rawRequest = request.toBytes();
        StreamReader<T> responseReader;

        Socket socket = borrowObject();
        try {
            socket.setTimeout(timeout == 0 ? readTimeout : timeout);
            socket.writeFully(rawRequest, 0, rawRequest.length);
            responseReader = new StreamReader<>(this, socket, BUFFER_SIZE, chunkBuilder);
            responseReader.readResponse();
        } catch (SocketTimeoutException | HttpException e) {
            invalidateObject(socket);
            throw e;
        } catch (IOException e) {
            // Stale connection? Retry on a fresh socket
            destroyObject(socket);
            socket = createObject();
            socket.writeFully(rawRequest, 0, rawRequest.length);
            responseReader = new StreamReader<>(this, socket, BUFFER_SIZE, chunkBuilder);
            responseReader.readResponse();
        }
        return responseReader;
    }

    interface ChunkBuilder<T> {

        T fromBytes(final byte[] bytes);
    }

    static class StreamReader<T> implements Iterator<T>, Closeable {

        private final HttpClient client;
        private final ChunkBuilder<T> chunkBuilder;
        private Socket socket;
        private byte[] buf;
        private int length;
        private int pos;
        private boolean needRead;
        private boolean lastChunkReaded;
        private byte[] chunk;
        private boolean isAvailable;
        private boolean keepAlive;
        private Response response;

        StreamReader(final HttpClient client, Socket socket, int bufferSize,
            ChunkBuilder<T> chunkBuilder) throws IOException {
            this.socket = socket;
            this.buf = new byte[bufferSize];
            this.length = socket.read(buf, 0, bufferSize, 0);
            needRead = true;
            chunk = null;
            lastChunkReaded = false;
            this.client = client;
            this.chunkBuilder = chunkBuilder;
        }

        void readResponse() throws IOException, HttpException {
            String responseHeader = readLine();
            if (responseHeader.length() <= 9) {
                throw new HttpException("Invalid response header: " + responseHeader);
            }

            Response response = new Response(responseHeader.substring(9));
            for (String header; !(header = readLine()).isEmpty(); ) {
                response.addHeader(header);
            }
            keepAlive = !"close".equalsIgnoreCase(response.getHeader("Connection: "));
            this.response = response;
            if (response.getStatus() != 200 || !"chunked"
                .equalsIgnoreCase(response.getHeader("Transfer-Encoding: "))) {
                isAvailable = false;
                close();
                return;
            }
            isAvailable = true;
        }

        public Response getResponse() {
            return response;
        }

        public boolean isAvailable() {
            return isAvailable;
        }

        @Override
        public void close() {
            if (!isAvailable) {
                return;
            }
            if (keepAlive) {
                client.returnObject(socket);
            } else {
                client.invalidateObject(socket);
            }
            isAvailable = false;
        }

        @Override
        public boolean hasNext() {
            if (!isAvailable) {
                return false;
            }
            try {
                readIfNeed();
            } catch (IOException | HttpException e) {
                close();
                return false;
            }
            return chunk != null;
        }

        @Override
        public T next() {
            needRead = true;
            final var result = chunkBuilder.fromBytes(chunk);
            chunk = null;
            return result;
        }

        private void readIfNeed() throws IOException, HttpException {
            if (!needRead) {
                return;
            }
            needRead = false;
            readSingleChunk();
        }

        private String readLine() throws IOException, HttpException {
            byte[] buf = this.buf;
            int pos = this.pos;
            int lineStart = pos;

            do {
                if (pos == length) {
                    if (pos >= buf.length) {
                        throw new HttpException("Line too long");
                    }
                    length += socket.read(buf, pos, buf.length - pos, 0);
                }
            } while (buf[pos++] != '\n');

            this.pos = pos;
            return Utf8.read(buf, lineStart, pos - lineStart - 2);
        }

        private void readSingleChunk() throws IOException, HttpException {
            if (lastChunkReaded) {
                return;
            }
            int chunkSize = Integer.parseInt(readLine(), 16);
            if (chunkSize == 0) {
                readLine();
                lastChunkReaded = true;
                close();
                return;
            }

            chunk = new byte[chunkSize];

            int contentBytes = length - pos;
            if (contentBytes < chunkSize) {
                System.arraycopy(buf, pos, chunk, 0, contentBytes);
                socket.readFully(chunk, contentBytes, chunkSize - contentBytes);
                pos = 0;
                length = 0;
            } else {
                System.arraycopy(buf, pos, chunk, 0, chunkSize);
                pos += chunkSize;
                if (pos + 128 >= buf.length) {
                    System.arraycopy(buf, pos, buf, 0, length -= pos);
                    pos = 0;
                }
            }

            readLine();
        }

    }

}
