package org.elece.storage.file;

import org.elece.exception.StorageException;

import java.nio.file.Path;

public interface FileHandlerFactory {
    FileHandler getFileHandler(Path path) throws StorageException;
}
