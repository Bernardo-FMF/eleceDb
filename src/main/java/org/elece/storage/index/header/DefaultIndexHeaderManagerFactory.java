package org.elece.storage.index.header;

import org.elece.exception.StorageException;

import java.nio.file.Path;

public class DefaultIndexHeaderManagerFactory implements IndexHeaderManagerFactory {
    @Override
    public IndexHeaderManager getInstance(Path path) throws StorageException {
        return new JsonIndexHeaderManager(path);
    }
}
