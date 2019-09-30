package ru.mail.polis.dao;

import java.io.IOException;

public class DAOException extends IOException {
    public DAOException(String message, Throwable cause) {
        super(message, cause);
    }
}
