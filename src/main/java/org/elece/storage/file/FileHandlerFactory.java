package org.elece.storage.file;

import java.io.IOException;
import java.nio.file.Path;

public interface FileHandlerFactory {
    FileHandler getFileHandler(Path path) throws IOException;
}
