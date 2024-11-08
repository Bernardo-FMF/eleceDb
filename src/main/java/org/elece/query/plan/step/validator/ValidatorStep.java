package org.elece.query.plan.step.validator;

import org.elece.exception.*;

public abstract class ValidatorStep<V> {
    public abstract boolean validate(V value) throws SchemaException, StorageException, BTreeException,
                                                     InterruptedTaskException, FileChannelException;
}
