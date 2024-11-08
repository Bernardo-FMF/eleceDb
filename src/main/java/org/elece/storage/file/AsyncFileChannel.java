package org.elece.storage.file;

import org.elece.exception.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class AsyncFileChannel implements FileChannel {
    AsynchronousFileChannel fileChannel;

    public AsyncFileChannel(Path path, ExecutorService executorService) throws StorageException {
        try {
            this.fileChannel = AsynchronousFileChannel.open(path,
                    Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
                    executorService);
        } catch (IOException exception) {
            throw new StorageException(DbError.CHANNEL_OPENING_ERROR, exception.getMessage());
        }
    }

    public AsyncFileChannel(Path path) throws StorageException {
        try {
            this.fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException exception) {
            throw new StorageException(DbError.CHANNEL_OPENING_ERROR, exception.getMessage());
        }
    }

    @Override
    public CompletableFuture<Integer> writeAsync(long position, byte[] bytes) throws FileChannelException {
        validateChannelState();
        CompletableFuture<Integer> result = new CompletableFuture<>();

        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.flip();

        fileChannel.write(byteBuffer, position, null, new CompletionHandler<>() {
            @Override
            public void completed(Integer writtenBytes, Object attachment) {
                result.complete(writtenBytes);
            }

            @Override
            public void failed(Throwable exception, Object attachment) {
                result.completeExceptionally(new StorageException(DbError.FILE_WRITE_ERROR, "Failed to write to file"));
            }
        });

        return result;
    }

    @Override
    public Integer write(long position, byte[] bytes) throws FileChannelException, StorageException,
                                                             InterruptedTaskException {
        return handleFuture(writeAsync(position, bytes));
    }

    @Override
    public CompletableFuture<byte[]> readAsync(long position, int size) throws FileChannelException {
        validateChannelState();
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        fileChannel.read(buffer, position, buffer, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer readBytes, ByteBuffer attachment) {
                        attachment.flip();
                        byte[] data = new byte[attachment.limit()];
                        attachment.get(data);
                        result.complete(data);
                    }

                    @Override
                    public void failed(Throwable exception, ByteBuffer attachment) {
                        attachment.flip();
                        result.completeExceptionally(new StorageException(DbError.FILE_READ_ERROR, "Failed to read file"));
                    }
                }
        );
        return result;
    }

    @Override
    public byte[] read(long position, int size) throws FileChannelException, InterruptedTaskException {
        return handleFuture(readAsync(position, size));
    }

    @Override
    public CompletableFuture<Long> allocateAsync(long position, int size) throws FileChannelException,
                                                                                 StorageException {
        validateChannelState();
        CompletableFuture<Long> future = new CompletableFuture<>();

        int readSize = (int) (size() - position);
        allocateAsync(size).whenComplete((_, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
                return;
            }
            try {
                readAsync(position, readSize).whenComplete((readBytes, throwable1) -> {
                    if (throwable1 != null) {
                        future.completeExceptionally(throwable1);
                        return;
                    }
                    try {
                        writeAsync(position, new byte[readBytes.length]).whenComplete((_, throwable2) -> {
                            if (throwable2 != null) {
                                future.completeExceptionally(throwable2);
                                return;
                            }
                            try {
                                writeAsync(position + size, readBytes).whenComplete((_, throwable3) -> {
                                    if (throwable3 != null) {
                                        future.completeExceptionally(throwable3);
                                        return;
                                    }
                                    future.complete(position);
                                });
                            } catch (FileChannelException exception) {
                                throw new RuntimeDbException(exception.getDbError(), exception.getMessage());
                            }
                        });
                    } catch (FileChannelException exception) {
                        throw new RuntimeDbException(exception.getDbError(), exception.getMessage());
                    }
                });
            } catch (FileChannelException exception) {
                throw new RuntimeDbException(exception.getDbError(), exception.getMessage());
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<Long> allocateAsync(int size) throws FileChannelException, StorageException {
        CompletableFuture<Long> result = new CompletableFuture<>();
        long fileSize = size();
        fileChannel.write(ByteBuffer.allocate(size), fileSize, null, new CompletionHandler<>() {
            @Override
            public void completed(Integer writtenBytes, Object attachment) {
                result.complete(fileSize);
            }

            @Override
            public void failed(Throwable throwable, Object attachment) {
                result.completeExceptionally(new StorageException(DbError.FAILED_TO_ALLOCATE_BYTES_ERROR, "Failed to allocate bytes in file"));
            }
        });
        return result;
    }

    @Override
    public Long allocate(long position, int size) throws FileChannelException, StorageException,
                                                         InterruptedTaskException {
        return handleFuture(allocateAsync(position, size));
    }

    @Override
    public Long allocate(int size) throws FileChannelException, StorageException, InterruptedTaskException {
        return handleFuture(allocateAsync(size));
    }

    @Override
    public Long size() throws FileChannelException, StorageException {
        validateChannelState();
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new StorageException(DbError.FILE_READ_ERROR, "Failed to read file size");
        }
    }

    @Override
    public void close() throws FileChannelException {
        validateChannelState();
        try {
            fileChannel.close();
        } catch (IOException exception) {
            throw new FileChannelException(DbError.FAIL_TO_CLOSE_CHANNEL_ERROR, exception.getMessage());
        }
    }

    @Override
    public boolean supportsAsync() {
        return true;
    }

    private void validateChannelState() throws FileChannelException {
        if (!fileChannel.isOpen()) {
            throw new FileChannelException(DbError.CHANNEL_CLOSED_ERROR, "File channel is closed");
        }
    }

    private <V> V handleFuture(CompletableFuture<V> future) throws InterruptedTaskException {
        try {
            return future.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new InterruptedTaskException(DbError.TASK_INTERRUPTED_ERROR, "File IO operation interrupted");
        } catch (ExecutionException e) {
            throw new InterruptedTaskException(DbError.TASK_ENDED_IN_FAILURE_ERROR, "File IO operation failed");
        }
    }
}
