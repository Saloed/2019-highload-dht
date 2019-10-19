package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.*;

import ru.mail.polis.Record;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class RocksDAOImpl implements DAOWithTimestamp {

    private final RocksDB db;

    private RocksDAOImpl(final RocksDB db) {
        this.db = db;
    }

    static DAOWithTimestamp create(@NotNull final File data) throws IOException {
        RocksDB.loadLibrary();
        try {
            final var options = new Options()
                    .setCreateIfMissing(true)
                    .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
            final var db = RocksDB.open(options, data.getAbsolutePath());
            return new RocksDAOImpl(db);
        } catch (RocksDBException exception) {
            throw new DAOException("Can't obtain db instance", exception);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final var fromByteArray = ByteBufferUtils.toArrayShifted(from);
        final var iterator = db.newIterator();
        iterator.seek(fromByteArray);
        return new RocksRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key)
            throws IOException, NoSuchElementException {
        final var record = getRecord(key).withoutTimestamp();
        if (record == null) {
            throw new NoSuchElementExceptionLite("Key is not present: " + key.toString());
        }
        return record.getValue();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value)
            throws IOException {
        final var record = RecordWithTimestamp.create(key, value);
        upsertRecord(record);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final var record = RecordWithTimestamp.empty(key);
        upsertRecord(record);
    }

    @Override
    public void compact() throws IOException {
        try {
            db.compactRange();
        } catch (RocksDBException exception) {
            throw new DAOException("Error while compact", exception);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            db.syncWal();
            db.closeE();
        } catch (RocksDBException exception) {
            throw new DAOException("Error while close", exception);
        }
    }

    @NotNull
    @Override
    public RecordWithTimestamp getRecord(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final var keyByteArray = ByteBufferUtils.toArrayShifted(key);
        try {
            final var valueByteArray = db.get(keyByteArray);
            if (valueByteArray == null) {
                throw new NoSuchElementExceptionLite("Key is not present: " + key.toString());
            }
            return RecordWithTimestamp.fromRawBytes(keyByteArray, valueByteArray);
        } catch (RocksDBException exception) {
            throw new DAOException("Error while get", exception);
        }
    }

    @Override
    public void upsertRecord(@NotNull RecordWithTimestamp record) throws IOException {
        final var keyByteArray = ByteBufferUtils.copyArrayShifted(record.getKey());
        final var valueByteArray = record.toRawBytes();
        try {
            db.put(keyByteArray, valueByteArray);
        } catch (RocksDBException exception) {
            throw new DAOException("Error while upsert", exception);
        }
    }

    public static class RocksRecordIterator implements Iterator<Record>, Closeable {

        private final RocksIterator iterator;

        RocksRecordIterator(@NotNull final RocksIterator iterator) {
            this.iterator = iterator;
            skipEmptyRecords();
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new IllegalStateException("Iterator is exhausted");
            }
            skipEmptyRecords();
            final var record = getRecord();
            iterator.next();
            return record.withoutTimestamp();
        }

        private RecordWithTimestamp getRecord() {
            final var keyByteArray = iterator.key();
            final var valueByteArray = iterator.value();
            final var keyUnshifted = ByteBufferUtils.copyArrayShiftedBack(keyByteArray);
            return RecordWithTimestamp.fromRawBytes(keyUnshifted, valueByteArray);
        }

        private void skipEmptyRecords() {
            while (hasNext() && getRecord().isEmpty()) {
                iterator.next();
            }
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }

}
