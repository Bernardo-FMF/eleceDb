package org.elece.storage.file;

import org.elece.exception.StorageException;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultFileHandlerFactory implements FileHandlerFactory {
    private ExecutorService executorService;

    private static DefaultFileHandlerFactory factoryInstance;

    private DefaultFileHandlerFactory() {
        // private constructor
    }

    public static synchronized DefaultFileHandlerFactory getInstance() {
        return getInstance(null);
    }

    public static synchronized DefaultFileHandlerFactory getInstance(int threadCount) {
        return getInstance(Executors.newFixedThreadPool(threadCount));
    }

    public static synchronized DefaultFileHandlerFactory getInstance(ExecutorService executorService) {
        if (factoryInstance == null) {
            factoryInstance = new DefaultFileHandlerFactory();
            if (executorService != null) {
                factoryInstance.executorService = executorService;
            }
        }
        return factoryInstance;
    }

    @Override
    public synchronized FileHandler getFileHandler(Path path) throws StorageException {
        if (executorService == null) {
            return new FileHandler(path);
        }
        return new FileHandler(path, executorService);
    }
}
