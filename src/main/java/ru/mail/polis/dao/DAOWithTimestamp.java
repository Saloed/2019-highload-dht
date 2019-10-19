package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public interface DAOWithTimestamp extends DAO {
    @NotNull
    RecordWithTimestamp getRecord(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException;

    void upsertRecord(@NotNull final ByteBuffer key, @NotNull final RecordWithTimestamp record) throws IOException;
}
