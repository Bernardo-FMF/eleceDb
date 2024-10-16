package org.elece.query;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Index;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.sql.parser.statement.CreateIndexStatement;

import java.io.IOException;

public class CreateIndexQueryExecutor implements QueryExecutor {
    private final String name;
    private final String table;
    private final String column;

    public CreateIndexQueryExecutor(CreateIndexStatement statement) {
        this.name = statement.getName();
        this.table = statement.getTable();
        this.column = statement.getColumn();
    }

    @Override
    public int execute(SchemaManager schemaManager) throws SchemaException, IOException, BTreeException, SerializationException, StorageException, DeserializationException, DbException {
        return schemaManager.createIndex(table, new Index(name, column));
    }
}
