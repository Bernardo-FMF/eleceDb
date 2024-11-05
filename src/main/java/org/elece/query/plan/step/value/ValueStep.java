package org.elece.query.plan.step.value;

import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.storage.StorageException;

import java.util.Optional;

public abstract class ValueStep {
    public abstract Optional<byte[]> next() throws SchemaException, StorageException, ParserException,
                                                   SerializationException;
}
