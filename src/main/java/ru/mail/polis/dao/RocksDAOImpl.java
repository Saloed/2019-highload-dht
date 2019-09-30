package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class RocksDAOImpl implements DAO {
    private final RocksDB db;

    private RocksDAOImpl(RocksDB db) {
        this.db = db;
    }


    public static class RocksRecordIterator implements Iterator<Record>, AutoCloseable {

        private final RocksIterator iterator;

        RocksRecordIterator(@NotNull final RocksIterator iterator) {
            this.iterator = iterator;
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
            final var keyByteArray = iterator.key();
            final var valueByteArray = iterator.value();
            final var key = ByteBuffer.wrap(keyByteArray);
            final var value = ByteBuffer.wrap(valueByteArray);
            final var record = Record.of(key, value);
            iterator.next();
            return record;
        }

        @Override
        public void close() throws Exception {
            iterator.close();
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final var fromByteArray = from.array();
        final var iterator = db.newIterator();
        iterator.seek(fromByteArray);
        return new RocksRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final var keyByteArray = key.array();
        try {
            final var valueByteArray = db.get(keyByteArray);
            if (valueByteArray == null) {
                throw new NoSuchElementExceptionLite("Key is not present: " + key.toString());
            }
            return ByteBuffer.wrap(valueByteArray);
        } catch (RocksDBException exception) {
            throw new DAOException("Error while get", exception);
        }
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        final var keyByteArray = key.array();
        final var valueByteArray = value.array();
        try {
            db.put(keyByteArray, valueByteArray);
        } catch (RocksDBException exception) {
            throw new DAOException("Error while upsert", exception);
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        final var keyByteArray = key.array();
        try {
            db.delete(keyByteArray);
        } catch (RocksDBException exception) {
            throw new DAOException("Error while remove", exception);
        }
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

    static DAO create(File data) throws IOException {
        RocksDB.loadLibrary();
        try {
            final var comparator = new BytewiseComparator(new ComparatorOptions());
            final var options = new Options()
                    .setCreateIfMissing(true)
                    .setComparator(comparator);
            final var db = RocksDB.open(options, data.getAbsolutePath());
            return new RocksDAOImpl(db);
        } catch (RocksDBException exception) {
            throw new DAOException("Can't obtain db instance", exception);
        }
    }

}
