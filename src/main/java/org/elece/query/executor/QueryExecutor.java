package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface QueryExecutor {
    void execute(SchemaManager schemaManager) throws SchemaException, IOException, BTreeException,
                                                     SerializationException, StorageException, DeserializationException,
                                                     DbException, ExecutionException, InterruptedException,
                                                     ProtoException;
}
