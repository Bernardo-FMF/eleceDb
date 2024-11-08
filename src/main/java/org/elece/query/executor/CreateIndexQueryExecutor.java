package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Index;
import org.elece.exception.*;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.result.GenericQueryResultInfo;
import org.elece.query.result.builder.GenericQueryResultInfoBuilder;
import org.elece.sql.parser.statement.CreateIndexStatement;

public class CreateIndexQueryExecutor implements QueryExecutor {
    private final String name;
    private final String table;
    private final String column;
    private final StreamStep streamStep;

    public CreateIndexQueryExecutor(CreateIndexStatement statement, StreamStep streamStep) {
        this.name = statement.getName();
        this.table = statement.getTable();
        this.column = statement.getColumn();
        this.streamStep = streamStep;
    }

    @Override
    public void execute(SchemaManager schemaManager) throws SchemaException, BTreeException,
                                                            SerializationException, StorageException,
                                                            DeserializationException, DbException, ProtoException,
                                                            InterruptedTaskException, FileChannelException {
        int rowCount = schemaManager.createIndex(table, new Index(name, column));
        streamStep.stream(GenericQueryResultInfoBuilder.builder()
                .setQueryType(GenericQueryResultInfo.QueryType.CREATE_INDEX)
                .setAffectedRowCount(rowCount)
                .build());
    }
}
