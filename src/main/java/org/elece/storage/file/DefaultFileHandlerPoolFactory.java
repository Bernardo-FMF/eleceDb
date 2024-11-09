package org.elece.storage.file;

import org.elece.config.DbConfig;

import java.util.Objects;

public class DefaultFileHandlerPoolFactory implements FileHandlerPoolFactory {
    private final DbConfig dbConfig;

    private FileHandlerPool fileHandlerPool;

    public DefaultFileHandlerPoolFactory(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public FileHandlerPool getFileHandlerPool() {
        if (Objects.nonNull(fileHandlerPool)) {
            return fileHandlerPool;
        }

        if (dbConfig.getFileHandlerStrategy() == DbConfig.FileHandlerStrategy.UNLIMITED) {
            fileHandlerPool = new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(this.dbConfig.getFileHandlerPoolThreads()), dbConfig);
        } else {
            fileHandlerPool = new RestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(this.dbConfig.getFileHandlerPoolThreads()), this.dbConfig);
        }

        return fileHandlerPool;
    }
}
