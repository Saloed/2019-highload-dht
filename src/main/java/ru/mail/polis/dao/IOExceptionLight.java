package ru.mail.polis.dao;

import java.io.IOException;

public class IOExceptionLight extends IOException {

    private static final long serialVersionUID = 6769829250639411232L;

    public IOExceptionLight(final String message) {
        super(message);
    }

    public IOExceptionLight(final String message, final Throwable cause) {
        super(message, cause);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
