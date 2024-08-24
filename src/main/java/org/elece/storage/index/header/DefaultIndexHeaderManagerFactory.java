package org.elece.storage.index.header;

import org.elece.storage.error.StorageException;

import java.io.IOException;
import java.nio.file.Path;

public class DefaultIndexHeaderManagerFactory implements IndexHeaderManagerFactory {
    @Override
    public IndexHeaderManager getInstance(Path path) throws IOException, StorageException {
        return new JsonIndexHeaderManager(path);
    }
}
