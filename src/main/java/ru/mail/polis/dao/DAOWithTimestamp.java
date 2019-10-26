package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface DAOWithTimestamp extends DAO {

    /**
     * Retrieve record from storage by specified key.
     *
     * @param key of record
     * @return obtained record
     * @throws IOException if storage error occurred
     */
    @NotNull
    RecordWithTimestamp getRecord(@NotNull final ByteBuffer key) throws IOException;

    /**
     * Modify or create record in storage by given key.
     *
     * @param key    of record
     * @param record to create or modify
     * @throws IOException if storage error occurred
     */
    void upsertRecord(@NotNull final ByteBuffer key, @NotNull final RecordWithTimestamp record)
            throws IOException;

    Iterator<RecordWithTimestampAndKey> recordRange(@NotNull ByteBuffer from, @Nullable ByteBuffer to);
}
