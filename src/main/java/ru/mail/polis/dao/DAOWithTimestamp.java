package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public interface DAOWithTimestamp extends DAO {

    /**
     * Retrieve record from storage by specified key.
     *
     * @param key of record
     * @return obtained record
     * @throws IOException            if storage error occurred
     * @throws NoSuchElementException if no such key in storage
     */
    @NotNull
    RecordWithTimestamp getRecord(@NotNull final ByteBuffer key)
        throws IOException, NoSuchElementException;

    /**
     * Modify or create record in storage by given key.
     *
     * @param key    of record
     * @param record to create or modify
     * @throws IOException if storage error occurred
     */
    void upsertRecord(@NotNull final ByteBuffer key, @NotNull final RecordWithTimestamp record)
        throws IOException;
}
