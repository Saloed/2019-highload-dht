package ru.mail.polis.service.saloed;

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
     * @param request HTTP request
     * @return iterator over chunks
     * @throws InterruptedException something bad happens
     * @throws PoolException        socket pool exception occurred
     * @throws IOException          if network error occurred
     * @throws HttpException        if http format exception occurred
     */
    public synchronized Response invokeStream(final Request request,
        final StreamConsumer streamConsumer)
        throws InterruptedException, PoolException, IOException, HttpException {
        int method = request.getMethod();
        byte[] rawRequest = request.toBytes();
        StreamReader responseReader;

        Socket socket = borrowObject();
        boolean keepAlive = false;
        try {
            try {
                socket.setTimeout(timeout == 0 ? readTimeout : timeout);
                socket.writeFully(rawRequest, 0, rawRequest.length);
                responseReader = new StreamReader(socket, BUFFER_SIZE);
            } catch (SocketTimeoutException e) {
                throw e;
            } catch (IOException e) {
                // Stale connection? Retry on a fresh socket
                destroyObject(socket);
                socket = createObject();
                socket.writeFully(rawRequest, 0, rawRequest.length);
                responseReader = new StreamReader(socket, BUFFER_SIZE);
            }
            Response response = responseReader.readResponse(method);
            keepAlive = !"close".equalsIgnoreCase(response.getHeader("Connection: "));
            streamConsumer.consume(responseReader);
            return response;
        } finally {
            if (keepAlive) {
                returnObject(socket);
            } else {
                invalidateObject(socket);
            }
        }
    }

    interface StreamConsumer {

        void consume(final StreamReader stream)
            throws IOException, InterruptedException, PoolException, HttpException;
    }

    static class StreamReader implements Iterator<byte[]> {

        private Socket socket;
        private byte[] buf;
        private int length;
        private int pos;
        private boolean needRead;
        private boolean lastChunkReaded;
        private byte[] chunk;
        private boolean isAvailable;
        private Response response;

        StreamReader(Socket socket, int bufferSize)
            throws IOException {
            this.socket = socket;
            this.buf = new byte[bufferSize];
            this.length = socket.read(buf, 0, bufferSize, 0);
            isAvailable = false;
            needRead = true;
            chunk = null;
            lastChunkReaded = false;
        }

        Response readResponse(final int method) throws IOException, HttpException {
            String responseHeader = readLine();
            if (responseHeader.length() <= 9) {
                throw new HttpException("Invalid response header: " + responseHeader);
            }

            Response response = new Response(responseHeader.substring(9));
            for (String header; !(header = readLine()).isEmpty(); ) {
                response.addHeader(header);
            }

            if (method != Request.METHOD_HEAD && response.getStatus() != 204) {
                String contentLength = response.getHeader("Content-Length: ");
                if (contentLength != null) {
                    byte[] body = new byte[Integer.parseInt(contentLength)];
                    int contentBytes = length - pos;
                    System.arraycopy(buf, pos, body, 0, contentBytes);
                    if (contentBytes < body.length) {
                        socket.readFully(body, contentBytes, body.length - contentBytes);
                    }
                    response.setBody(body);
                } else if ("chunked".equalsIgnoreCase(response.getHeader("Transfer-Encoding: "))) {
                    isAvailable = true;
                } else {
                    throw new HttpException("Content-Length unspecified");
                }
            }
            this.response = response;
            return response;
        }

        public Response getResponse() {
            return response;
        }

        public boolean isNotAvailable() {
            return !isAvailable;
        }


        @Override
        public boolean hasNext() {
            if (!isAvailable) {
                return false;
            }
            try {
                readIfNeed();
            } catch (IOException | HttpException e) {
                return false;
            }
            return chunk != null;
        }

        @Override
        public byte[] next() {
            needRead = true;
            final var result = chunk;
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
