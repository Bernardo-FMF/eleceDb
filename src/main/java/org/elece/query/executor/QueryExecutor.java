package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.*;

/**
 * Represents a query that doesn't need a {@link org.elece.query.plan.QueryPlan}.
 * These queries are related to internal database management, like creating/deleting the database, creating/deleting a table or creating indexes.
 */
public interface QueryExecutor {
    void execute(SchemaManager schemaManager) throws SchemaException, BTreeException,
                                                     SerializationException, StorageException, DeserializationException,
                                                     DbException, ProtoException, InterruptedTaskException,
                                                     FileChannelException;
}
