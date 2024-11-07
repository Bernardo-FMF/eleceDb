package org.elece.query.plan;

import org.elece.exception.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface QueryPlan {
    void execute() throws ParserException, SerializationException, SchemaException, StorageException, IOException,
                          ExecutionException, InterruptedException, DbException, BTreeException,
                          DeserializationException, ProtoException;
}
