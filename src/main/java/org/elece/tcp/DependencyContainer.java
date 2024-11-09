package org.elece.tcp;

import org.elece.config.DbConfig;
import org.elece.db.DatabaseStorageManager;
import org.elece.db.DiskPageDatabaseStorageManager;
import org.elece.db.InMemoryReservedSlotTracer;
import org.elece.db.ReservedSlotTracer;
import org.elece.db.schema.JsonSchemaManager;
import org.elece.db.schema.SchemaManager;
import org.elece.exception.SchemaException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.DefaultColumnIndexManagerProvider;
import org.elece.query.QueryPlanner;
import org.elece.serializer.SerializerRegistry;
import org.elece.storage.file.DefaultFileHandlerPoolFactory;
import org.elece.storage.file.FileHandlerPoolFactory;
import org.elece.storage.index.DefaultIndexStorageManagerFactory;
import org.elece.storage.index.IndexStorageManagerFactory;
import org.elece.storage.index.header.DefaultIndexHeaderManagerFactory;
import org.elece.storage.index.header.IndexHeaderManagerFactory;

import java.util.Objects;

public class DependencyContainer {
    private final DbConfig dbConfig;
    private SchemaManager schemaManager;
    private ColumnIndexManagerProvider columnIndexManagerProvider;
    private DatabaseStorageManager databaseStorageManager;
    private IndexStorageManagerFactory indexStorageManagerFactory;
    private FileHandlerPoolFactory fileHandlerPoolFactory;
    private ReservedSlotTracer reservedSlotTracer;
    private IndexHeaderManagerFactory indexHeaderManagerFactory;
    private QueryPlanner queryPlanner;

    public DependencyContainer(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public SchemaManager getSchemaManager() throws SchemaException {
        if (Objects.isNull(schemaManager)) {
            schemaManager = new JsonSchemaManager(dbConfig, getColumnIndexManagerProvider(), getDatabaseStorageManager());
        }
        return schemaManager;
    }

    public DatabaseStorageManager getDatabaseStorageManager() {
        if (Objects.isNull(databaseStorageManager)) {
            databaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, getFileHandlerPoolFactory().getFileHandlerPool(), getReservedSlotTracer());
        }
        return databaseStorageManager;
    }

    public FileHandlerPoolFactory getFileHandlerPoolFactory() {
        if (Objects.isNull(fileHandlerPoolFactory)) {
            fileHandlerPoolFactory = new DefaultFileHandlerPoolFactory(dbConfig);
        }
        return fileHandlerPoolFactory;
    }

    public ReservedSlotTracer getReservedSlotTracer() {
        if (Objects.isNull(reservedSlotTracer)) {
            reservedSlotTracer = new InMemoryReservedSlotTracer();
        }
        return reservedSlotTracer;
    }

    public ColumnIndexManagerProvider getColumnIndexManagerProvider() {
        if (Objects.isNull(columnIndexManagerProvider)) {
            columnIndexManagerProvider = new DefaultColumnIndexManagerProvider(dbConfig, getIndexStorageManagerFactory());
        }
        return columnIndexManagerProvider;
    }

    public IndexStorageManagerFactory getIndexStorageManagerFactory() {
        if (Objects.isNull(indexStorageManagerFactory)) {
            indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(dbConfig, getFileHandlerPoolFactory(), getIndexHeaderManagerFactory());
        }
        return indexStorageManagerFactory;
    }

    public IndexHeaderManagerFactory getIndexHeaderManagerFactory() {
        if (Objects.isNull(indexHeaderManagerFactory)) {
            indexHeaderManagerFactory = new DefaultIndexHeaderManagerFactory();
        }
        return indexHeaderManagerFactory;
    }

    public SerializerRegistry getSerializerRegistry() {
        return SerializerRegistry.getInstance();
    }

    public QueryPlanner getQueryPlanner() throws SchemaException {
        if (Objects.isNull(queryPlanner)) {
            queryPlanner = new QueryPlanner(getSchemaManager(), getDatabaseStorageManager(), getColumnIndexManagerProvider(), getSerializerRegistry(), getFileHandlerPoolFactory().getFileHandlerPool(), dbConfig);
        }
        return queryPlanner;
    }
}
