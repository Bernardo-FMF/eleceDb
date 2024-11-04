package org.elece.query.plan.step.order;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.elece.config.DbConfig;
import org.elece.db.schema.model.Column;
import org.elece.exception.RuntimeDbException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.internal.*;
import org.elece.storage.file.FileHandlerPool;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class CacheableOrderStep<V extends Comparable<V>> extends OrderStep {
    private final List<Column> selectedColumns;
    private final Column orderByColumn;
    private final Long scanId;

    private final Integer rowSize;
    private Comparator<byte[]> comparator;

    private final FileHandlerPool fileHandlerPool;
    private final SerializerRegistry serializerRegistry;

    private final Cache<byte[], byte[]> rowCache;
    private final Queue<byte[]> orderedBuffer;

    private final List<TempFileWrapper> tempFiles;
    private final DbConfig dbConfig;

    public CacheableOrderStep(Order order,
                              List<Column> selectedColumns,
                              Column orderByColumn,
                              Long scanId,
                              FileHandlerPool fileHandlerPool,
                              SerializerRegistry serializerRegistry,
                              DbConfig dbConfig) {
        this.selectedColumns = selectedColumns;
        this.orderByColumn = orderByColumn;
        this.scanId = scanId;

        this.rowSize = selectedColumns.stream().map(column -> column.getSqlType().getSize()).reduce(0, Integer::sum);

        this.fileHandlerPool = fileHandlerPool;
        this.serializerRegistry = serializerRegistry;
        this.dbConfig = dbConfig;

        this.rowCache = CacheBuilder
                .newBuilder()
                .maximumSize(dbConfig.getDbQueryCacheSize())
                .initialCapacity(dbConfig.getDbQueryCacheSize() / 2)
                .build();

        Function<byte[], SqlValue<V>> valueExtractor = buildValueDeserializerFunction();

        comparator = (b1, b2) -> {
            SqlValue<V> b1Key = valueExtractor.apply(b1);
            SqlValue<V> b2Key = valueExtractor.apply(b2);

            return b1Key.compare(b2Key);
        };

        if (order == Order.Desc) {
            comparator = comparator.reversed();
        }

        this.orderedBuffer = new PriorityQueue<>(comparator);
        this.tempFiles = new ArrayList<>();
    }

    private Function<byte[], SqlValue<V>> buildValueDeserializerFunction() {
        Serializer<V> serializer = serializerRegistry.getSerializer(orderByColumn.getSqlType().getType());
        return switch (orderByColumn.getSqlType().getType()) {
            case Int -> (data) -> {
                try {
                    return (SqlValue<V>) new SqlNumberValue((Integer) serializer.deserialize(data, orderByColumn));
                } catch (DeserializationException exception) {
                    throw new RuntimeDbException(exception.getDbError());
                }
            };
            case Bool -> (data) -> {
                try {
                    return (SqlValue<V>) new SqlBoolValue((Boolean) serializer.deserialize(data, orderByColumn));
                } catch (DeserializationException exception) {
                    throw new RuntimeDbException(exception.getDbError());
                }
            };
            case Varchar -> (data) -> {
                try {
                    return (SqlValue<V>) new SqlStringValue((String) serializer.deserialize(data, orderByColumn));
                } catch (DeserializationException exception) {
                    throw new RuntimeDbException(exception.getDbError());
                }
            };
        };
    }

    private Path getTempFileName() {
        return Path.of(this.dbConfig.getBaseDbPath(), String.format("elece_%d_%d.query.bin", scanId, this.tempFiles.size() + 1));
    }

    @Override
    public void addToBuffer(byte[] data) {
        byte[] key = SerializationUtils.getValueOfField(selectedColumns, orderByColumn, data);

        orderedBuffer.add(key);
        rowCache.put(key, data);

        if (orderedBuffer.size() >= dbConfig.getDbQueryCacheSize()) {
            flush();
        }
    }

    @Override
    public void prepareBufferState() {
        if (tempFiles.isEmpty()) {
            return;
        }

        if (!orderedBuffer.isEmpty()) {
            flush();
        }
    }

    @Override
    public Iterator<byte[]> getIterator() {
        if (tempFiles.isEmpty()) {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return !orderedBuffer.isEmpty();
                }

                @Override
                public byte[] next() {
                    byte[] nextRowId = orderedBuffer.poll();
                    if (!Objects.isNull(nextRowId)) {
                        byte[] row = rowCache.getIfPresent(nextRowId);

                        rowCache.invalidate(nextRowId);
                        return row;
                    }
                    return new byte[0];
                }
            };
        }

        Serializer<V> serializer = serializerRegistry.getSerializer(orderByColumn.getSqlType().getType());
        return new DistributedSortingIterator(tempFiles, comparator, serializer.size(orderByColumn), rowSize);
    }

    @Override
    public void clearBuffer() {
        this.rowCache.invalidateAll();
        this.orderedBuffer.clear();

        if (!tempFiles.isEmpty()) {
            for (TempFileWrapper tempFile : tempFiles) {
                tempFile.getTempFileName().toFile().delete();

                fileHandlerPool.releaseFileHandler(tempFile.getTempFileName());
            }
        }

        tempFiles.clear();
    }

    private void flush() {
        Path tempFileName = getTempFileName();

        try {
            AsynchronousFileChannel tempChannel = fileHandlerPool.acquireFileHandler(tempFileName);

            Long position = 0L;
            while (!orderedBuffer.isEmpty()) {
                byte[] nextRowId = orderedBuffer.poll();
                byte[] row = rowCache.getIfPresent(nextRowId);

                byte[] concatenatedRow = new byte[nextRowId.length + rowSize];
                BinaryUtils.copyBytes(nextRowId, concatenatedRow, 0, 0, nextRowId.length);
                BinaryUtils.copyBytes(row, concatenatedRow, 0, nextRowId.length, rowSize);

                tempChannel.write(ByteBuffer.wrap(concatenatedRow), position * rowSize).get();

                position++;
            }

            Serializer<V> serializer = serializerRegistry.getSerializer(orderByColumn.getSqlType().getType());
            tempFiles.add(new TempFileWrapper(tempFileName, tempChannel, serializer.size(orderByColumn) + rowSize));
        } catch (InterruptedException | IOException | ExecutionException exception) {
            fileHandlerPool.releaseFileHandler(tempFileName);
            tempFiles.removeIf(tempFile -> tempFile.getTempFileName().equals(tempFileName));
        } finally {
            rowCache.invalidateAll();
            orderedBuffer.clear();
        }
    }

    private static class DistributedSortingIterator implements Iterator<byte[]> {
        private final List<TempFileWrapper> tempFiles;
        private final Queue<byte[]> valueQueue;
        private final Integer headerSize;
        private final Integer rowSize;

        public DistributedSortingIterator(List<TempFileWrapper> tempFiles,
                                          Comparator<byte[]> comparator,
                                          Integer headerSize,
                                          Integer rowSize) {
            this.tempFiles = tempFiles;
            this.headerSize = headerSize;
            this.rowSize = rowSize;

            Comparator<byte[]> headerComparator = (b1, b2) -> {
                byte[] b1Header = new byte[headerSize];
                byte[] b2Header = new byte[headerSize];
                BinaryUtils.copyBytes(b1, b1Header, 0, 0, headerSize);
                BinaryUtils.copyBytes(b2, b2Header, 0, 0, headerSize);

                return comparator.compare(b1Header, b2Header);
            };
            this.valueQueue = new PriorityQueue<>(headerComparator);
        }

        @Override
        public boolean hasNext() {
            return !tempFiles.isEmpty();
        }

        @Override
        public byte[] next() {
            valueQueue.clear();

            List<TempFileWrapper> filesToRemove = new ArrayList<>();
            for (TempFileWrapper tempFile : tempFiles) {
                Optional<byte[]> possibleValue = tempFile.getNextValue();
                if (possibleValue.isEmpty()) {
                    tempFile.clearNextValue();
                    filesToRemove.add(tempFile);
                    continue;
                }
                valueQueue.add(possibleValue.get());
            }
            tempFiles.removeAll(filesToRemove);

            byte[] nextValueWithHeader = valueQueue.poll();

            byte[] nextValue = new byte[rowSize];
            BinaryUtils.copyBytes(nextValueWithHeader, nextValue, headerSize, 0, rowSize);

            tempFiles.stream().filter(tempFile -> {
                Optional<byte[]> possibleValue = tempFile.getNextValue();
                return possibleValue.isPresent() && possibleValue.get() == nextValueWithHeader;
            }).findFirst().ifPresent(tempFileWrapper -> {
                tempFileWrapper.incrementReadPointer();
                tempFileWrapper.clearNextValue();
            });

            return nextValue;
        }
    }

    private static final class TempFileWrapper {
        private final Path tempFileName;
        private final AsynchronousFileChannel tempFileChannel;
        private final Integer rowSize;
        private Long readPointer;

        private byte[] nextValue;

        private TempFileWrapper(Path tempFileName,
                                AsynchronousFileChannel tempFileChannel,
                                Integer rowSize) {
            this.tempFileName = tempFileName;
            this.tempFileChannel = tempFileChannel;
            this.rowSize = rowSize;

            this.readPointer = 0L;
            this.nextValue = null;
        }

        public Path getTempFileName() {
            return tempFileName;
        }

        public void incrementReadPointer() {
            readPointer++;
        }

        public void clearNextValue() {
            this.nextValue = null;
        }

        public Optional<byte[]> getNextValue() {
            if (Objects.isNull(nextValue)) {
                byte[] newValue = new byte[rowSize];
                try {
                    ByteBuffer wrap = ByteBuffer.wrap(newValue);
                    Integer bytesRead = tempFileChannel.read(wrap, readPointer * rowSize).get();
                    if (!Objects.equals(bytesRead, rowSize)) {
                        return Optional.empty();
                    }
                    this.nextValue = newValue;
                    incrementReadPointer();
                } catch (InterruptedException | ExecutionException exception) {
                    return Optional.empty();
                }
            }

            byte[] nextValueCopy = this.nextValue;
            return Optional.of(nextValueCopy);
        }
    }
}
