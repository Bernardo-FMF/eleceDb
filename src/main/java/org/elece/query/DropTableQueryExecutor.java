package org.elece.query;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.sql.parser.statement.DropTableStatement;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DropTableQueryExecutor implements QueryExecutor {
    private final String tableName;

    public DropTableQueryExecutor(DropTableStatement statement) {
        this.tableName = statement.getTable();
    }

    @Override
    public int execute(SchemaManager schemaManager) throws SchemaException, IOException, BTreeException,
                                                           SerializationException, StorageException,
                                                           DeserializationException, DbException, ExecutionException,
                                                           InterruptedException {
        return schemaManager.deleteTable(tableName);
    }
}
