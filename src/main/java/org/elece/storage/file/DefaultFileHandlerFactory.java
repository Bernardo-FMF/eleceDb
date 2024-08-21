package org.elece.storage.file;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultFileHandlerFactory implements FileHandlerFactory {
    private ExecutorService executorService;

    private static DefaultFileHandlerFactory FACTORY_INSTANCE;

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
        if (FACTORY_INSTANCE == null) {
            FACTORY_INSTANCE = new DefaultFileHandlerFactory();
            if (executorService != null) {
                FACTORY_INSTANCE.executorService = executorService;
            }
        }
        return FACTORY_INSTANCE;
    }

    @Override
    public synchronized FileHandler getFileHandler(Path path) throws IOException {
        if (executorService == null) {
            return new FileHandler(path);
        }
        return new FileHandler(path, executorService);
    }
}
