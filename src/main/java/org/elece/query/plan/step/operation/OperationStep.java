package org.elece.query.plan.step.operation;

import org.elece.exception.*;

public abstract class OperationStep<V> {
    public abstract boolean execute(V value) throws BTreeException, StorageException, SchemaException, DbException,
                                                    SerializationException, DeserializationException,
                                                    InterruptedTaskException, FileChannelException;
}
