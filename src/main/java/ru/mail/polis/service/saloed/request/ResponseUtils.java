package ru.mail.polis.service.saloed.request;

import one.nio.http.Response;

public class ResponseUtils {


    private ResponseUtils() {
    }

    /**
     * HTTP response with status: 504 Not Enough Replicas.
     *
     * @return HTTP response
     */
    public static Response notEnoughReplicas() {
        return new Response("504 Not Enough Replicas", Response.EMPTY);
    }

    /**
     * HTTP response with status: 202 Accepted.
     *
     * @return HTTP response
     */
    public static Response accepted() {
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    /**
     * HTTP response with status: 201 Created.
     *
     * @return HTTP response
     */
    public static Response created() {
        return new Response(Response.CREATED, Response.EMPTY);
    }

    /**
     * HTTP response with status: 404 Not found.
     *
     * @return HTTP response
     */
    public static Response notFound() {
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }
}
