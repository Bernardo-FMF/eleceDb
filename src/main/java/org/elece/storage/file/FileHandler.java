package org.elece.storage.file;

import org.elece.exception.DbError;
import org.elece.exception.StorageException;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a wrapper class for the file descriptor.
 * It mostly serves as a state manager for it, so we can keep track of the current thread usages {@link FileHandler#usageCount} and if the channel is closed.
 */
public class FileHandler {
    private final AsynchronousFileChannel fileChannel;
    private int usageCount = 0;
    private volatile boolean isClosed = false;

    public FileHandler(Path path, ExecutorService executorService) throws StorageException {
        try {
            this.fileChannel = AsynchronousFileChannel.open(path,
                    Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
                    executorService);
        } catch (IOException exception) {
            throw new StorageException(DbError.CHANNEL_OPENING_ERROR, exception.getMessage());
        }
    }

    public FileHandler(Path path) throws StorageException {
        try {
            this.fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException exception) {
            throw new StorageException(DbError.CHANNEL_OPENING_ERROR, exception.getMessage());
        }
    }

    public synchronized void incrementUsage() {
        if (this.isClosed) {
            throw new IllegalStateException("File handler has been closed or is closing.");
        }
        usageCount++;
    }

    public synchronized void decrementUsage() {
        if (--this.usageCount == 0) {
            notifyAll();
        }
    }

    public AsynchronousFileChannel getChannel() {
        return fileChannel;
    }

    public int getUsageCount() {
        return usageCount;
    }

    public void closeChannel(long timeout, TimeUnit timeUnit) throws StorageException {
        this.isClosed = true;
        synchronized (this) {
            while (usageCount > 0) {
                try {
                    wait(timeUnit.toMillis(timeout));
                } catch (InterruptedException exception) {
                    throw new StorageException(DbError.TASK_INTERRUPTED_ERROR, exception.getMessage());
                }
            }

            usageCount = 0;
            try {
                fileChannel.close();
            } catch (IOException exception) {
                throw new StorageException(DbError.CHANNEL_CLOSING_ERROR, "Failed to close file channel");
            }
        }
    }
}
