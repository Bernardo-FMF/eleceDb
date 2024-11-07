package org.elece.db.schema;

import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface SchemaManager {
    Schema getSchema();

    void createSchema(String dbName) throws IOException, SchemaException;

    int deleteSchema() throws IOException, SchemaException, ExecutionException, InterruptedException, StorageException, DbException;

    void createTable(Table table) throws IOException, SchemaException, StorageException;

    <K extends Number & Comparable<K>> int deleteTable(String tableName) throws IOException, SchemaException, ExecutionException, InterruptedException, StorageException, DbException;

    <K extends Number & Comparable<K>> int createIndex(String tableName, Index index) throws IOException, SchemaException, StorageException, DbException, DeserializationException, BTreeException, SerializationException;
}
