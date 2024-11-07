package org.elece.query.plan.step.validator;

import org.elece.exception.BTreeException;
import org.elece.exception.SchemaException;
import org.elece.exception.StorageException;

public abstract class ValidatorStep<V> {
    public abstract boolean validate(V value) throws SchemaException, StorageException, BTreeException;
}
