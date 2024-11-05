package org.elece.query.plan.step.validator;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.storage.StorageException;

public abstract class ValidatorStep<V> {
    public abstract boolean validate(V value) throws SchemaException, StorageException, BTreeException;
}
