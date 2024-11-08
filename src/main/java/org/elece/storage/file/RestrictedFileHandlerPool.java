package org.elece.storage.file;

import org.elece.config.DbConfig;
import org.elece.exception.DbError;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * This implementation represents a more limited way to open file descriptors.
 * We require this because unix based operating systems place a limit on the number of open file descriptors that a process may have at any given time.
 * So to remedy this, when we receive a request to acquire a file descriptor but the limit has been reached,
 * we can use a semaphore to wait for the moment a slot opened up and immediately acquire it.
 */
public class RestrictedFileHandlerPool implements FileHandlerPool {
    private final Map<String, FileHandler> fileHandlers;
    private final Semaphore semaphore;
    private final DefaultFileHandlerFactory fileHandlerFactory;
    private final DbConfig dbConfig;

    public RestrictedFileHandlerPool(DefaultFileHandlerFactory fileHandlerFactory, DbConfig dbConfig) {
        this.fileHandlerFactory = fileHandlerFactory;
        fileHandlers = new ConcurrentHashMap<>(dbConfig.getFileDescriptorAcquisitionSize());
        semaphore = new Semaphore(dbConfig.getFileDescriptorAcquisitionSize());
        this.dbConfig = dbConfig;
    }

    @Override
    public FileChannel acquireFileHandler(Path path) throws StorageException, InterruptedTaskException {
        FileHandler fileHandler;

        synchronized (fileHandlers) {
            fileHandler = fileHandlers.get(path.toString());
            if (Objects.isNull(fileHandler)) {
                try {
                    if (!semaphore.tryAcquire(dbConfig.getAcquisitionTimeoutTime(), dbConfig.getTimeoutUnit())) {
                        throw new StorageException(DbError.TASK_INTERRUPTED_ERROR, String.format("Timeout while waiting to acquire file channel for %s", path));
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedTaskException(DbError.TASK_INTERRUPTED_ERROR, exception.getMessage());
                }
                fileHandler = fileHandlerFactory.getFileHandler(path);
                fileHandlers.put(path.toString(), fileHandler);
            }
            fileHandler.incrementUsage();
        }

        return fileHandler.getChannel();
    }

    @Override
    public void releaseFileHandler(Path path) throws StorageException, InterruptedTaskException, FileChannelException {
        FileHandler fileHandler = fileHandlers.get(path.toString());
        if (fileHandler != null) {
            fileHandler.decrementUsage();
            if (fileHandler.getUsageCount() <= 0) {
                semaphore.release();
                fileHandlers.remove(path.toString());
                fileHandler.closeChannel(dbConfig.getCloseTimeoutTime(), dbConfig.getTimeoutUnit());
            }
        }
    }

    @Override
    public void closeAll() throws InterruptedTaskException, FileChannelException {
        for (Map.Entry<String, FileHandler> entry : fileHandlers.entrySet()) {
            FileHandler fileHandler = entry.getValue();
            fileHandler.closeChannel(dbConfig.getCloseTimeoutTime(), dbConfig.getTimeoutUnit());
        }
    }
}
