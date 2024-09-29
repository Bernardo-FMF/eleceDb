package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.storage.StorageException;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Table;
import org.elece.storage.index.IndexStorageManagerFactory;

public abstract class ColumnIndexManagerProvider {
    protected final DbConfig dbConfig;
    protected final IndexStorageManagerFactory indexStorageManagerFactory;

    public ColumnIndexManagerProvider(DbConfig dbConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        this.dbConfig = dbConfig;
        this.indexStorageManagerFactory = indexStorageManagerFactory;
    }

    public abstract IndexManager<?, ?> getIndexManager(Table table, Column column) throws SchemaException, StorageException;

    public abstract void clearIndexManager(Table table, Column column);
}
