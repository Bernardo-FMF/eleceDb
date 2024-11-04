package org.elece.query;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.proto.TcpException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.result.GenericQueryResultInfo;
import org.elece.query.result.builder.GenericQueryResultInfoBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DropDbQueryExecutor implements QueryExecutor {
    private final StreamStep streamStep;

    public DropDbQueryExecutor(StreamStep streamStep) {
        this.streamStep = streamStep;
    }

    @Override
    public void execute(SchemaManager schemaManager) throws SchemaException, IOException, BTreeException,
                                                            SerializationException, StorageException,
                                                            DeserializationException, DbException, ExecutionException,
                                                            InterruptedException, TcpException {
        int rowCount = schemaManager.deleteSchema();
        streamStep.stream(GenericQueryResultInfoBuilder.builder()
                .setQueryType(GenericQueryResultInfo.QueryType.DROP_DB)
                .setAffectedRowCount(rowCount)
                .build());
    }
}
