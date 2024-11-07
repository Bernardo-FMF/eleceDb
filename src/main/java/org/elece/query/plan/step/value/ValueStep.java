package org.elece.query.plan.step.value;

import org.elece.exception.ParserException;
import org.elece.exception.SchemaException;
import org.elece.exception.SerializationException;
import org.elece.exception.StorageException;

import java.util.Optional;

public abstract class ValueStep {
    public abstract Optional<byte[]> next() throws SchemaException, StorageException, ParserException,
                                                   SerializationException;
}
