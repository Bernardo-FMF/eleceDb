package org.elece.storage.file;

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

    public FileHandler(Path path, ExecutorService executorService) throws IOException {
        this.fileChannel = AsynchronousFileChannel.open(path,
                Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
                executorService);
    }

    public FileHandler(Path path) throws IOException {
        this.fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    public synchronized void incrementUsage() {
        if (this.isClosed) {
            throw new IllegalStateException("FileHandler has been closed or is closing.");
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

    public void closeChannel(long timeout, TimeUnit timeUnit) throws IOException {
        this.isClosed = true;
        synchronized (this) {
            try {
                while (usageCount > 0) {
                    try {
                        wait(timeUnit.toMillis(timeout));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                usageCount = 0;
                fileChannel.close();
            }
        }
    }
}
