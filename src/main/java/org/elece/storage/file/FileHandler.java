package org.elece.storage.file;

import org.elece.exception.DbError;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a wrapper class for the file descriptor.
 * It mostly serves as a state manager for it, so we can keep track of the current thread usages {@link FileHandler#usageCount} and if the channel is closed.
 */
public class FileHandler {
    private final FileChannel fileChannel;
    private int usageCount = 0;
    private volatile boolean isClosed = false;

    public FileHandler(Path path, ExecutorService executorService) throws StorageException {
        fileChannel = new AsyncFileChannel(path, executorService);
    }

    public FileHandler(Path path) throws StorageException {
        fileChannel = new AsyncFileChannel(path);

    }

    public synchronized void incrementUsage() {
        if (this.isClosed) {
            throw new IllegalStateException("File channel has been closed or is closing.");
        }
        usageCount++;
    }

    public synchronized void decrementUsage() {
        if (--this.usageCount == 0) {
            notifyAll();
        }
    }

    public FileChannel getChannel() {
        return fileChannel;
    }

    public int getUsageCount() {
        return usageCount;
    }

    public void closeChannel(long timeout, TimeUnit timeUnit) throws InterruptedTaskException,
                                                                     FileChannelException {
        this.isClosed = true;
        synchronized (this) {
            while (usageCount > 0) {
                try {
                    wait(timeUnit.toMillis(timeout));
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedTaskException(DbError.TASK_INTERRUPTED_ERROR, exception.getMessage());
                }
            }

            usageCount = 0;
            fileChannel.close();

        }
    }
}
