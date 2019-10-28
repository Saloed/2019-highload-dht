package ru.mail.polis.service.saloed.request;

import one.nio.http.Response;

public class ResponseUtils {


    private ResponseUtils() {
    }

    public static Response notEnoughReplicas() {
        return new Response("504 Not Enough Replicas", Response.EMPTY);
    }

    public static Response accepted() {
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    public static Response created() {
        return new Response(Response.CREATED, Response.EMPTY);
    }

    public static Response notFound() {
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }
}
