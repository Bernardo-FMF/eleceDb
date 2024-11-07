package org.elece.storage.file;

import org.elece.exception.StorageException;

import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;

public interface FileHandlerPool {
    AsynchronousFileChannel acquireFileHandler(Path path) throws StorageException;

    void releaseFileHandler(Path path) throws StorageException;

    void closeAll() throws StorageException;
}