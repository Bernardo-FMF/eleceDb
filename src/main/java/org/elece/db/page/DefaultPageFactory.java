package org.elece.db.page;

import org.elece.config.DbConfig;
import org.elece.exception.db.DbException;
import org.elece.exception.db.type.PageAcquisitionError;
import org.elece.storage.file.FileHandlerPool;
import org.elece.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class DefaultPageFactory implements PageFactory {
    private final DbConfig dbConfig;
    private final FileHandlerPool fileHandlerPool;
    private final Function<Integer, Path> dbFileFunction;

    public DefaultPageFactory(DbConfig dbConfig, FileHandlerPool fileHandlerPool, Function<Integer, Path> dbFileFunction) {
        this.dbConfig = dbConfig;
        this.fileHandlerPool = fileHandlerPool;
        this.dbFileFunction = dbFileFunction;
    }

    @Override
    public Page getPage(PageTitle pageTitle) throws DbException {
        try {
            Path path = dbFileFunction.apply(pageTitle.getChunk());

            AsynchronousFileChannel fileChannel = fileHandlerPool.acquireFileHandler(path);
            int size = this.dbConfig.getDbPageSize();
            int offset = pageTitle.getPageNumber() * size;

            byte[] data = FileUtils.readBytes(fileChannel, offset, size).get();
            fileHandlerPool.releaseFileHandler(path);

            return new Page(pageTitle.getPageNumber(), pageTitle.getChunk(), data);
        } catch (InterruptedException | IOException | ExecutionException e) {
            throw new DbException(new PageAcquisitionError(dbFileFunction.apply(pageTitle.getChunk()).toString()));
        }
    }
}
