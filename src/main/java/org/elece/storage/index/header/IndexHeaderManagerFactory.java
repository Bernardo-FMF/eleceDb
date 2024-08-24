package org.elece.storage.index.header;

import org.elece.storage.error.StorageException;

import java.io.IOException;
import java.nio.file.Path;

public interface IndexHeaderManagerFactory {
    IndexHeaderManager getInstance(Path path) throws IOException, StorageException;
}
