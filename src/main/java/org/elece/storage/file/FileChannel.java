package org.elece.storage.file;

import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;

import java.util.concurrent.CompletableFuture;

public interface FileChannel {
    CompletableFuture<Integer> writeAsync(long position, byte[] bytes) throws FileChannelException, StorageException;

    Integer write(long position, byte[] bytes) throws FileChannelException, StorageException, InterruptedTaskException;

    CompletableFuture<byte[]> readAsync(long position, int size) throws FileChannelException, StorageException;

    byte[] read(long position, int size) throws FileChannelException, StorageException, InterruptedTaskException;

    CompletableFuture<Long> allocateAsync(long position, int size) throws FileChannelException, StorageException;

    CompletableFuture<Long> allocateAsync(int size) throws FileChannelException, StorageException;

    Long allocate(long position, int size) throws FileChannelException, StorageException, InterruptedTaskException;

    Long allocate(int size) throws FileChannelException, StorageException, InterruptedTaskException;

    Long size() throws FileChannelException, StorageException;

    void close() throws FileChannelException;

    boolean supportsAsync();
}
