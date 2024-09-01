package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.storage.StorageException;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Table;
import org.elece.storage.index.header.IndexHeaderManagerFactory;

public abstract class IndexStorageManagerFactory {
    protected final DbConfig dbConfig;
    protected final IndexHeaderManagerFactory indexHeaderManagerFactory;

    protected IndexStorageManagerFactory(DbConfig dbConfig, IndexHeaderManagerFactory indexHeaderManagerFactory) {
        this.dbConfig = dbConfig;
        this.indexHeaderManagerFactory = indexHeaderManagerFactory;
    }

    public abstract IndexStorageManager create(Table table, Column column) throws StorageException;
}