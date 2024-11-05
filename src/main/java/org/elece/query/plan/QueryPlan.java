package org.elece.query.plan;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.proto.TcpException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.storage.StorageException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface QueryPlan {
    void execute() throws ParserException, SerializationException, SchemaException, StorageException, IOException,
                          ExecutionException, InterruptedException, DbException, BTreeException,
                          DeserializationException, TcpException;
}
