package org.elece.sql.db.schema;

import org.elece.exception.schema.SchemaException;
import org.elece.exception.storage.StorageException;
import org.elece.sql.db.schema.model.Index;
import org.elece.sql.db.schema.model.Schema;
import org.elece.sql.db.schema.model.Table;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface SchemaManager {
    Schema getSchema();

    void createSchema(String dbName) throws IOException, SchemaException;

    void createTable(Table table) throws IOException, SchemaException;

    void deleteTable(String tableName) throws IOException, SchemaException, ExecutionException, InterruptedException, StorageException;

    void createIndex(String tableName, Index index) throws IOException, SchemaException;
}
