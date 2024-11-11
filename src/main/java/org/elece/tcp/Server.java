package org.elece.tcp;

import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.ServerException;
import org.elece.exception.StorageException;

import java.io.IOException;

public interface Server {
    void start() throws InterruptedTaskException, StorageException, FileChannelException, ServerException;

    void close() throws IOException, InterruptedTaskException, StorageException, FileChannelException;
}
