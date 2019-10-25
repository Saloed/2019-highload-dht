package ru.mail.polis.service.saloed;

import one.nio.http.Response;

public class ResponseUtils {

    public static final Response NOT_ENOUGH_REPLICAS = new Response("504 Not Enough Replicas",
        Response.EMPTY);
    public static final Response ACCEPTED = new Response(Response.ACCEPTED, Response.EMPTY);
    public static final Response CREATED = new Response(Response.CREATED, Response.EMPTY);
    public static final Response NOT_FOUND = new Response(Response.NOT_FOUND, Response.EMPTY);

    private ResponseUtils() {
    }
}
