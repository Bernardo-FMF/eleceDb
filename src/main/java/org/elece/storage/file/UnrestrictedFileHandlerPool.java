package org.elece.storage.file;

import org.elece.config.IDbConfig;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This implementation represents an unrestricted way to open file descriptors.
 * Unlike {@link RestrictedFileHandlerPool}, using this implementation has the risk of encountering an IO exception referencing
 * that the limit of open file descriptors has been reached.
 * In this scenario, there is no fallback and the acquire operation will fail.
 */
public class UnrestrictedFileHandlerPool implements IFileHandlerPool {
    private final Map<String, FileHandler> fileHandlers;
    private final FileHandlerFactory fileHandlerFactory;
    private final IDbConfig dbConfig;

    public UnrestrictedFileHandlerPool(FileHandlerFactory fileHandlerFactory, IDbConfig dbConfig) {
        this.fileHandlerFactory = fileHandlerFactory;
        this.fileHandlers = new ConcurrentHashMap<>();
        this.dbConfig = dbConfig;
    }

    @Override
    public AsynchronousFileChannel acquireFileHandler(Path path) {
        FileHandler fileHandler = fileHandlers.computeIfAbsent(path.toString(), tempPath -> {
            try {
                return fileHandlerFactory.getFileHandler(Path.of(tempPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        fileHandler.incrementUsage();
        return fileHandler.getChannel();
    }

    @Override
    public void releaseFileHandler(Path path) {
        FileHandler fileHandler = fileHandlers.get(path.toString());
        if (fileHandler != null) {
            fileHandler.decrementUsage();
        }
    }

    @Override
    public void closeAll() {
        fileHandlers.forEach((pathName, fileHandler) -> {
            try {
                fileHandler.closeChannel(dbConfig.getCloseTimeoutTime(), dbConfig.getTimeoutUnit());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
