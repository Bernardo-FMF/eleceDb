package org.elece.db.schema;

import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;

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
