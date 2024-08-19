package org.elece.storage.index.header;

import org.elece.storage.error.StorageException;
import org.elece.storage.file.IFileHandlerPool;

import java.io.IOException;
import java.nio.file.Path;

public class BaseIndexHeaderManagerFactory implements IndexHeaderManagerFactory {
    @Override
    public IndexHeaderManager getInstance(Path path, IFileHandlerPool fileHandlerPool) throws IOException, StorageException {
        return new BinaryIndexHeaderManager(path, fileHandlerPool);
    }
}
