package org.elece.sql.db.schema;

import org.elece.sql.db.schema.model.Index;
import org.elece.sql.db.schema.model.Schema;
import org.elece.sql.db.schema.model.Table;

import java.io.IOException;

public interface SchemaManager {
    Schema getSchema();

    void createTable(Table table) throws IOException;

    void deleteTable(String tableName) throws IOException;

    void createIndex(String tableName, Index index) throws IOException;
}
