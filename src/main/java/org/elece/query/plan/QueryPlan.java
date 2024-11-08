package org.elece.query.plan;

import org.elece.exception.*;

public interface QueryPlan {
    void execute() throws ParserException, SerializationException, SchemaException, StorageException, DbException,
                          BTreeException, DeserializationException, ProtoException, InterruptedTaskException,
                          FileChannelException;
}
