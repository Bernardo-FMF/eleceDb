package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.SchemaException;
import org.elece.exception.StorageException;
import org.elece.memory.Pointer;
import org.elece.storage.index.IndexStorageManagerFactory;

public abstract class ColumnIndexManagerProvider {
    protected final DbConfig dbConfig;
    protected final IndexStorageManagerFactory indexStorageManagerFactory;

    protected ColumnIndexManagerProvider(DbConfig dbConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        this.dbConfig = dbConfig;
        this.indexStorageManagerFactory = indexStorageManagerFactory;
    }

    public abstract <K extends Comparable<K>, T extends Number> IndexManager<K, T> getIndexManager(Table table, Column column) throws SchemaException, StorageException;

    public abstract <K extends Comparable<K>> IndexManager<K, Pointer> getClusterIndexManager(Table table) throws SchemaException, StorageException;

    public abstract void clearIndexManager(Table table, Column column);
}
