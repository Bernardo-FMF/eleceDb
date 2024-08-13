package org.elece.storage.file;

import java.io.IOException;
import java.nio.file.Path;

public interface IFileHandlerFactory {
    FileHandler getFileHandler(Path path) throws IOException;
}
