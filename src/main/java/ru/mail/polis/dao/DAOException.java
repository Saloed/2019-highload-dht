package ru.mail.polis.dao;

import java.io.IOException;

public class DAOException extends IOException {
    public DAOException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
