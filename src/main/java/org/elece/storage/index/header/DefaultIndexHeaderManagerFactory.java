package org.elece.storage.index.header;

import org.elece.storage.error.StorageException;
import org.elece.storage.file.FileHandlerPool;

import java.io.IOException;
import java.nio.file.Path;

public class DefaultIndexHeaderManagerFactory implements IndexHeaderManagerFactory {
    @Override
    public IndexHeaderManager getInstance(Path path, FileHandlerPool fileHandlerPool) throws IOException, StorageException {
        return new BinaryIndexHeaderManager(path, fileHandlerPool);
    }
}
