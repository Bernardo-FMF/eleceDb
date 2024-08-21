package org.elece.storage.file;

import org.elece.storage.error.StorageException;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;

public interface FileHandlerPool {
    AsynchronousFileChannel acquireFileHandler(Path path) throws InterruptedException, IOException;

    void releaseFileHandler(Path path);

    void closeAll() throws StorageException;
}