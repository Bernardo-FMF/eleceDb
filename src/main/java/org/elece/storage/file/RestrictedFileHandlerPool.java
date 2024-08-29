package org.elece.storage.file;

import org.elece.config.DbConfig;
import org.elece.storage.error.StorageException;
import org.elece.storage.error.type.InternalStorageError;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Map;
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
    public AsynchronousFileChannel acquireFileHandler(Path path) throws InterruptedException, IOException {
        FileHandler fileHandler;

        synchronized (fileHandlers) {
            fileHandler = fileHandlers.get(path.toString());
            if (fileHandler == null) {
                if (!semaphore.tryAcquire(dbConfig.getAcquisitionTimeoutTime(), dbConfig.getTimeoutUnit())) {
                    throw new IllegalStateException("Timeout while waiting to acquire file handler for " + path);
                }
                fileHandler = fileHandlerFactory.getFileHandler(path);
                fileHandlers.put(path.toString(), fileHandler);
            }
            fileHandler.incrementUsage();
        }

        return fileHandler.getChannel();
    }

    @Override
    public void releaseFileHandler(Path path) {
        FileHandler fileHandler = fileHandlers.get(path.toString());
        if (fileHandler != null) {
            fileHandler.decrementUsage();
            if (fileHandler.getUsageCount() <= 0) {
                try {
                    semaphore.release();
                    fileHandlers.remove(path.toString());
                    fileHandler.closeChannel(dbConfig.getCloseTimeoutTime(), dbConfig.getTimeoutUnit());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void closeAll() throws StorageException {
        for (Map.Entry<String, FileHandler> entry : fileHandlers.entrySet()) {
            FileHandler fileHandler = entry.getValue();
            try {
                fileHandler.closeChannel(dbConfig.getCloseTimeoutTime(), dbConfig.getTimeoutUnit());
            } catch (IOException e) {
                throw new StorageException(new InternalStorageError(""));
            }
        }
    }
}
