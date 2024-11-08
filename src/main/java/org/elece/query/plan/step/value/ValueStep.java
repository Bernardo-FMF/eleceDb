package org.elece.query.plan.step.value;

import org.elece.exception.*;

import java.util.Optional;

public abstract class ValueStep {
    public abstract Optional<byte[]> next() throws SchemaException, StorageException, ParserException,
                                                   SerializationException, InterruptedTaskException,
                                                   FileChannelException;
}
