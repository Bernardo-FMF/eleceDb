package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.StorageException;
import org.elece.index.IndexId;
import org.elece.storage.file.DefaultFileHandlerFactory;
import org.elece.storage.file.FileHandlerPool;
import org.elece.storage.file.RestrictedFileHandlerPool;
import org.elece.storage.file.UnrestrictedFileHandlerPool;
import org.elece.storage.index.header.IndexHeaderManagerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultIndexStorageManagerFactory extends IndexStorageManagerFactory {
    private final Map<String, IndexStorageManager> storageManagers;
    private FileHandlerPool fileHandlerPool;

    public DefaultIndexStorageManagerFactory(DbConfig dbConfig, IndexHeaderManagerFactory indexHeaderManagerFactory) {
        super(dbConfig, indexHeaderManagerFactory);

        this.storageManagers = new ConcurrentHashMap<>();
    }

    private synchronized FileHandlerPool getFileHandlerPool() {
        if (fileHandlerPool != null) {
            return fileHandlerPool;
        }

        if (dbConfig.getFileHandlerStrategy() == DbConfig.FileHandlerStrategy.UNLIMITED) {
            fileHandlerPool = new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(this.dbConfig.getFileHandlerPoolThreads()), dbConfig);
        } else {
            fileHandlerPool = new RestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(this.dbConfig.getFileHandlerPoolThreads()), this.dbConfig);
        }

        return fileHandlerPool;
    }

    @Override
    public IndexStorageManager create(IndexId indexId) {
        String managerId = indexId.asString();
        return this.storageManagers.computeIfAbsent(managerId, _ -> {
            DbConfig.IndexStorageManagerStrategy indexStorageManagerStrategy = dbConfig.getIndexStorageManagerStrategy();
            try {
                if (indexStorageManagerStrategy == DbConfig.IndexStorageManagerStrategy.ORGANIZED) {
                    return new OrganizedIndexStorageManager(managerId, indexHeaderManagerFactory, dbConfig, getFileHandlerPool());
                } else {
                    return new CompactIndexStorageManager(managerId, indexHeaderManagerFactory, dbConfig, getFileHandlerPool());
                }
            } catch (StorageException | IOException exception) {
                // TODO fix exception
                throw new RuntimeException(exception);
            }
        });
    }
}
