package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Table;
import org.elece.storage.error.StorageException;
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

    private String getManagerId(int tableId, int columnId) {
        return "%d_%d".formatted(tableId, columnId);
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
    public IndexStorageManager create(Table table, Column column) {
        String managerId = getManagerId(table.getId(), column.getId());
        return this.storageManagers.computeIfAbsent(managerId, key -> {
            DbConfig.IndexStorageManagerStrategy indexStorageManagerStrategy = dbConfig.getIndexStorageManagerStrategy();
            try {
                if (indexStorageManagerStrategy == DbConfig.IndexStorageManagerStrategy.ORGANIZED) {
                    return new OrganizedIndexStorageManager(managerId, indexHeaderManagerFactory, dbConfig, getFileHandlerPool());
                } else {
                    return new CompactIndexStorageManager(managerId, indexHeaderManagerFactory, dbConfig, getFileHandlerPool());
                }
            } catch (StorageException | IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
