package org.elece.query.plan.step.operation;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class OperationStep<V> {
    public abstract boolean execute(V value) throws BTreeException, StorageException, SchemaException, IOException,
                                                    ExecutionException, InterruptedException, DbException,
                                                    SerializationException;
}
