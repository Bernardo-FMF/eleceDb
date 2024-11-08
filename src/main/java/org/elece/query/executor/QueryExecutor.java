package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.*;

public interface QueryExecutor {
    void execute(SchemaManager schemaManager) throws SchemaException, BTreeException,
                                                     SerializationException, StorageException, DeserializationException,
                                                     DbException, ProtoException, InterruptedTaskException,
                                                     FileChannelException;
}
