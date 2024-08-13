package org.elece.storage.file;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;

public interface IFileHandlerPool {
    AsynchronousFileChannel acquireFileHandler(Path path) throws InterruptedException, IOException;

    void releaseFileHandler(Path path);

    void closeAll();
}