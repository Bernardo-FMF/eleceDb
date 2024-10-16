package org.elece.query;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DropDbQueryExecutor implements QueryExecutor {
    public DropDbQueryExecutor() {
    }

    @Override
    public int execute(SchemaManager schemaManager) throws SchemaException, IOException, BTreeException, SerializationException, StorageException, DeserializationException, DbException, ExecutionException, InterruptedException {
        return schemaManager.deleteSchema();
    }
}
