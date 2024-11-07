package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.*;
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
                                                            InterruptedException, ProtoException {
        int rowCount = schemaManager.deleteSchema();
        streamStep.stream(GenericQueryResultInfoBuilder.builder()
                .setQueryType(GenericQueryResultInfo.QueryType.DROP_DB)
                .setAffectedRowCount(rowCount)
                .build());
    }
}
