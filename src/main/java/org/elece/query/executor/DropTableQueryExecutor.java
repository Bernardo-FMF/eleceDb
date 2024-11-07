package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.*;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.result.GenericQueryResultInfo;
import org.elece.query.result.builder.GenericQueryResultInfoBuilder;
import org.elece.sql.parser.statement.DropTableStatement;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DropTableQueryExecutor implements QueryExecutor {
    private final String tableName;
    private final StreamStep streamStep;

    public DropTableQueryExecutor(DropTableStatement statement, StreamStep streamStep) {
        this.tableName = statement.getTable();
        this.streamStep = streamStep;
    }

    @Override
    public void execute(SchemaManager schemaManager) throws SchemaException, IOException, BTreeException,
                                                            SerializationException, StorageException,
                                                            DeserializationException, DbException, ExecutionException,
                                                            InterruptedException, ProtoException {
        int rowCount = schemaManager.deleteTable(tableName);
        streamStep.stream(GenericQueryResultInfoBuilder.builder()
                .setQueryType(GenericQueryResultInfo.QueryType.DROP_TABLE)
                .setAffectedRowCount(rowCount)
                .build());
    }
}
