package org.elece.db.page;

import org.elece.config.DbConfig;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;
import org.elece.storage.file.FileChannel;
import org.elece.storage.file.FileHandlerPool;

import java.nio.file.Path;
import java.util.function.Function;

public class DefaultPageFactory implements PageFactory {
    private final DbConfig dbConfig;
    private final FileHandlerPool fileHandlerPool;
    private final Function<Integer, Path> dbFileFunction;

    public DefaultPageFactory(DbConfig dbConfig, FileHandlerPool fileHandlerPool,
                              Function<Integer, Path> dbFileFunction) {
        this.dbConfig = dbConfig;
        this.fileHandlerPool = fileHandlerPool;
        this.dbFileFunction = dbFileFunction;
    }

    @Override
    public Page getPage(PageTitle pageTitle) throws InterruptedTaskException, StorageException,
                                                    FileChannelException {
        Path path = dbFileFunction.apply(pageTitle.getChunk());

        FileChannel fileChannel = fileHandlerPool.acquireFileHandler(path);
        int size = this.dbConfig.getDbPageSize();
        int offset = pageTitle.getPageNumber() * size;

        byte[] data = fileChannel.read(offset, size);
        fileHandlerPool.releaseFileHandler(path);

        return new Page(pageTitle.getPageNumber(), pageTitle.getChunk(), data);
    }
}
