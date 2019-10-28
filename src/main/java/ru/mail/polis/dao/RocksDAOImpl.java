package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.jetbrains.annotations.Nullable;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import ru.mail.polis.Record;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestampAndKey;

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
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterator = db.newIterator();
        return new RocksRecordIterator(iterator, from, null);
    }

    @NotNull
    @Override
    public Iterator<Record> range(@NotNull ByteBuffer from, @Nullable ByteBuffer to) {
        final var iterator = db.newIterator();
        return new RocksRecordIterator(iterator, from, to);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key)
            throws IOException, NoSuchElementException {
        final var record = getRecord(key);
        if (!record.isValue()) {
            throw new NoSuchElementExceptionLite("Key is not present: " + key.toString());
        }
        return record.getValue();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value)
            throws IOException {
        final var record = RecordWithTimestamp.fromValue(value, System.currentTimeMillis());
        upsertRecord(key, record);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final var record = RecordWithTimestamp.tombstone(System.currentTimeMillis());
        upsertRecord(key, record);
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
    public RecordWithTimestamp getRecord(@NotNull final ByteBuffer key) throws IOException {
        final var keyBytes = ByteBufferUtils.toArrayShifted(key);
        try {
            final var valueBytes = db.get(keyBytes);
            return RecordWithTimestamp.fromBytes(valueBytes);
        } catch (RocksDBException exception) {
            throw new DAOException("Error while get", exception);
        }
    }

    @Override
    public void upsertRecord(@NotNull final ByteBuffer key,
                             @NotNull final RecordWithTimestamp record) throws IOException {
        final var keyBytes = ByteBufferUtils.toArrayShifted(key);
        final var valueBytes = record.toRawBytes();
        try {
            db.put(keyBytes, valueBytes);
        } catch (RocksDBException exception) {
            throw new DAOException("Error while put", exception);
        }
    }

    @Override
    public Iterator<RecordWithTimestampAndKey> recordRange(@NotNull ByteBuffer from, @Nullable ByteBuffer to) {
        final var iterator = db.newIterator();
        return new RocksRecordWithTimestampIterator(iterator, from, to);
    }

    public static abstract class RocksDAOIterator<T> implements Iterator<T>, Closeable {
        private final RocksIterator iterator;
        @Nullable
        private final ByteBuffer upperBound;

        RocksDAOIterator(@NotNull final RocksIterator iterator, @NotNull final ByteBuffer lowerBound, @Nullable final ByteBuffer upperBound) {
            this.iterator = initIterator(iterator, lowerBound);
            final var upperBoundShifted = upperBound == null ? null : ByteBufferUtils.toArrayShifted(upperBound);
            this.upperBound = upperBound == null ? null : ByteBuffer.wrap(upperBoundShifted);
        }


        public RocksIterator getIterator() {
            return iterator;
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            if (!iterator.isValid()) return false;
            if (upperBound == null) return true;
            final var key = ByteBuffer.wrap(iterator.key());
            return key.compareTo(upperBound) < 0;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new IllegalStateException("Iterator is exhausted");
            }
            final var record = construct(iterator.key(), iterator.value());
            iterator.next();
            return record;
        }

        protected abstract T construct(final byte[] key, final byte[] value);

        private static RocksIterator initIterator(final RocksIterator iterator, final ByteBuffer lowerBound) {
            final var fromByteArray = ByteBufferUtils.toArrayShifted(lowerBound);
            iterator.seek(fromByteArray);
            return iterator;
        }

    }

    private static class RocksRecordWithTimestampIterator extends RocksDAOIterator<RecordWithTimestampAndKey> {

        RocksRecordWithTimestampIterator(@NotNull final RocksIterator iterator, @NotNull final ByteBuffer lowerBound, @Nullable final ByteBuffer upperBound) {
            super(iterator, lowerBound, upperBound);
        }

        @Override
        protected RecordWithTimestampAndKey construct(byte[] key, byte[] value) {
            final var keyBuffer = ByteBufferUtils.fromArrayShifted(key);
            final var valueRecord = RecordWithTimestamp.fromBytes(value);
            return RecordWithTimestampAndKey.fromKeyValue(keyBuffer, valueRecord);
        }
    }

    private static class RocksRecordIterator extends RocksDAOIterator<Record> {

        private RocksRecordIterator(@NotNull final RocksIterator iterator, @NotNull final ByteBuffer lowerBound, @Nullable final ByteBuffer upperBound) {
            super(iterator, lowerBound, upperBound);
            skipEmptyRecords();
        }

        @Override
        public boolean hasNext() {
            return super.hasNext();
        }

        @Override
        public Record next() {
            final var result = super.next();
            skipEmptyRecords();
            return result;
        }

        @Override
        protected Record construct(byte[] key, byte[] value) {
            final var keyBuffer = ByteBufferUtils.fromArrayShifted(key);
            final var valueBuffer = RecordWithTimestamp.fromBytes(value).getValue();
            return Record.of(keyBuffer, valueBuffer);
        }

        void skipEmptyRecords() {
            while (getIterator().isValid() && RecordWithTimestamp.recordIsEmpty(getIterator().value())) {
                getIterator().next();
            }
        }

    }

}
