package org.elece.db.schema;

import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;

public interface SchemaManager {
    Schema getSchema();

    void createSchema(String dbName) throws SchemaException;

    int deleteSchema() throws SchemaException, StorageException, DbException, InterruptedTaskException,
                              FileChannelException;

    void createTable(Table table) throws SchemaException, StorageException;

    <K extends Number & Comparable<K>> int deleteTable(String tableName) throws SchemaException, StorageException,
                                                                                DbException, InterruptedTaskException,
                                                                                FileChannelException;

    <K extends Number & Comparable<K>> int createIndex(String tableName, Index index) throws SchemaException,
                                                                                             StorageException,
                                                                                             DbException,
                                                                                             DeserializationException,
                                                                                             BTreeException,
                                                                                             SerializationException,
                                                                                             InterruptedTaskException,
                                                                                             FileChannelException;
}
