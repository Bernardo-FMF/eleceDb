package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.DbError;
import org.elece.exception.StorageException;
import org.elece.index.IndexId;
import org.elece.storage.file.FileHandlerPoolFactory;
import org.elece.storage.index.header.IndexHeaderManagerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultIndexStorageManagerFactory extends IndexStorageManagerFactory {
    private final FileHandlerPoolFactory fileHandlerPoolFactory;
    private final Map<String, IndexStorageManager> storageManagers;

    public DefaultIndexStorageManagerFactory(DbConfig dbConfig, FileHandlerPoolFactory fileHandlerPoolFactory,
                                             IndexHeaderManagerFactory indexHeaderManagerFactory) {
        super(dbConfig, indexHeaderManagerFactory);

        this.storageManagers = new ConcurrentHashMap<>();
        this.fileHandlerPoolFactory = fileHandlerPoolFactory;
    }

    @Override
    public IndexStorageManager create(IndexId indexId) throws StorageException {
        String managerId = indexId.asString();
        if (this.storageManagers.containsKey(managerId)) {
            return this.storageManagers.get(managerId);
        } else {
            DbConfig.IndexStorageManagerStrategy indexStorageManagerStrategy = dbConfig.getIndexStorageManagerStrategy();
            try {
                IndexStorageManager indexStorageManager;
                if (indexStorageManagerStrategy == DbConfig.IndexStorageManagerStrategy.ORGANIZED) {
                    indexStorageManager = new OrganizedIndexStorageManager(managerId, indexHeaderManagerFactory, dbConfig, fileHandlerPoolFactory.getFileHandlerPool());
                } else {
                    indexStorageManager = new CompactIndexStorageManager(managerId, indexHeaderManagerFactory, dbConfig, fileHandlerPoolFactory.getFileHandlerPool());
                }
                this.storageManagers.put(managerId, indexStorageManager);
                return indexStorageManager;
            } catch (IOException exception) {
                throw new StorageException(DbError.INDEX_STORAGE_MANAGER_CREATION_ERROR, "Failed to create index storage manager");
            }
        }
    }
}
