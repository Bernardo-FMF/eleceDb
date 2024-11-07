package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.StorageException;
import org.elece.index.IndexId;
import org.elece.storage.index.header.IndexHeaderManagerFactory;

public abstract class IndexStorageManagerFactory {
    protected final DbConfig dbConfig;
    protected final IndexHeaderManagerFactory indexHeaderManagerFactory;

    protected IndexStorageManagerFactory(DbConfig dbConfig, IndexHeaderManagerFactory indexHeaderManagerFactory) {
        this.dbConfig = dbConfig;
        this.indexHeaderManagerFactory = indexHeaderManagerFactory;
    }

    public abstract IndexStorageManager create(IndexId indexId) throws StorageException;
}