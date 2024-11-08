package org.elece.storage.file;

import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;

import java.nio.file.Path;

public interface FileHandlerPool {
    FileChannel acquireFileHandler(Path path) throws StorageException, InterruptedTaskException;

    void releaseFileHandler(Path path) throws StorageException, InterruptedTaskException, FileChannelException;

    void closeAll() throws StorageException, InterruptedTaskException, FileChannelException;
}