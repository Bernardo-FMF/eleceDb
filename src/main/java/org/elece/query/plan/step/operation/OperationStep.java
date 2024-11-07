package org.elece.query.plan.step.operation;

import org.elece.exception.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class OperationStep<V> {
    public abstract boolean execute(V value) throws BTreeException, StorageException, SchemaException, IOException,
                                                    ExecutionException, InterruptedException, DbException,
                                                    SerializationException, DeserializationException;
}
