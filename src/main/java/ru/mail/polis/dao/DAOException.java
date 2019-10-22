package ru.mail.polis.dao;

public class DAOException extends IOExceptionLight {

    private static final long serialVersionUID = 6769829250639411882L;

    public DAOException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
