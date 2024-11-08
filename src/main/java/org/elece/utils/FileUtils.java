package org.elece.utils;

import org.elece.exception.DbError;
import org.elece.exception.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FileUtils {
    private FileUtils() {
        // private constructor
    }

    public static Long getFileSize(AsynchronousFileChannel asynchronousFileChannel) throws StorageException {
        try {
            return asynchronousFileChannel.size();
        } catch (IOException exception) {
            throw new StorageException(DbError.FILE_READ_ERROR, "Failed to read file size");
        }
    }

    public static byte[] readBytes(AsynchronousFileChannel asynchronousFileChannel, long position, int size) throws
                                                                                                             StorageException {
        try {
            return readBytesAsync(asynchronousFileChannel, position, size).get();
        } catch (InterruptedException | ExecutionException exception) {
            throw new StorageException(DbError.FILE_READ_ERROR, "Failed to read file");
        }
    }

    public static CompletableFuture<byte[]> readBytesAsync(AsynchronousFileChannel asynchronousFileChannel,
                                                           long position, int size) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        asynchronousFileChannel.read(
                buffer,
                position,
                buffer, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment) {
                        attachment.flip();
                        byte[] data = new byte[attachment.limit()];
                        attachment.get(data);
                        future.complete(data);
                    }

                    @Override
                    public void failed(Throwable exception, ByteBuffer attachment) {
                        attachment.flip();
                        future.completeExceptionally(new StorageException(DbError.FILE_READ_ERROR, "Failed to read file size"));
                    }
                }
        );
        return future;
    }

    public static Integer write(AsynchronousFileChannel asynchronousFileChannel, long position, byte[] content) throws
                                                                                                                StorageException {
        try {
            return writeAsync(asynchronousFileChannel, position, content).get();
        } catch (InterruptedException | ExecutionException exception) {
            throw new StorageException(DbError.FILE_WRITE_ERROR, "Failed to write bytes in file");
        }
    }

    public static CompletableFuture<Integer> writeAsync(AsynchronousFileChannel asynchronousFileChannel, long position,
                                                        byte[] content) {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        ByteBuffer byteBuffer = ByteBuffer.allocate(content.length);
        byteBuffer.put(content);
        byteBuffer.flip();

        asynchronousFileChannel.write(byteBuffer, position, null, new CompletionHandler<>() {
            @Override
            public void completed(Integer result, Object attachment) {
                future.complete(result);
            }

            @Override
            public void failed(Throwable exception, Object attachment) {
                future.completeExceptionally(exception);
            }
        });

        return future;
    }

    public static Long allocate(AsynchronousFileChannel asynchronousFileChannel, int size) throws StorageException {
        try {
            return allocateAsync(asynchronousFileChannel, size).get();
        } catch (InterruptedException | ExecutionException exception) {
            throw new StorageException(DbError.FILE_WRITE_ERROR, "Failed to allocate bytes in file");
        }
    }

    public static CompletableFuture<Long> allocateAsync(AsynchronousFileChannel asynchronousFileChannel,
                                                        int size) throws StorageException {
        CompletableFuture<Long> future = new CompletableFuture<>();
        long fileSize = getFileSize(asynchronousFileChannel);
        asynchronousFileChannel.write(ByteBuffer.allocate(size), fileSize, null, new CompletionHandler<>() {
            @Override
            public void completed(Integer result, Object attachment) {
                future.complete(fileSize);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    public static Long allocate(AsynchronousFileChannel asynchronousFileChannel, long position, int size) throws
                                                                                                          StorageException {
        try {
            return allocateAsync(asynchronousFileChannel, position, size).get();
        } catch (InterruptedException | ExecutionException exception) {
            throw new StorageException(DbError.FILE_WRITE_ERROR, "Failed to allocate bytes in file");
        }
    }

    public static CompletableFuture<Long> allocateAsync(AsynchronousFileChannel asynchronousFileChannel, long position,
                                                        int size) throws StorageException {
        CompletableFuture<Long> future = new CompletableFuture<>();

        int readSize = (int) (getFileSize(asynchronousFileChannel) - position);
        allocateAsync(asynchronousFileChannel, size).whenComplete((_, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
                return;
            }
            FileUtils.readBytesAsync(asynchronousFileChannel, position, readSize).whenComplete((readBytes, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                    return;
                }
                FileUtils.writeAsync(asynchronousFileChannel, position, new byte[readBytes.length]).whenComplete((_, throwable2) -> {
                    if (throwable2 != null) {
                        future.completeExceptionally(throwable2);
                        return;
                    }
                    FileUtils.writeAsync(asynchronousFileChannel, position + size, readBytes).whenComplete((_, throwable3) -> {
                        if (throwable3 != null) {
                            future.completeExceptionally(throwable3);
                            return;
                        }
                        future.complete(position);
                    });
                });
            });
        });

        return future;
    }
}
