package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.result.GenericQueryResultInfo;
import org.elece.query.result.builder.GenericQueryResultInfoBuilder;
import org.elece.sql.parser.statement.CreateTableStatement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CreateTableQueryExecutor implements QueryExecutor {
    private final String tableName;
    private final List<Column> columns;
    private final StreamStep streamStep;

    public CreateTableQueryExecutor(CreateTableStatement statement, StreamStep streamStep) {
        this.tableName = statement.getName();
        this.columns = statement.getColumns();
        this.streamStep = streamStep;
    }

    @Override
    public void execute(SchemaManager schemaManager) throws SchemaException, IOException, BTreeException,
                                                            SerializationException, StorageException,
                                                            DeserializationException, DbException, ExecutionException,
                                                            InterruptedException, ProtoException {
        schemaManager.createTable(new Table(tableName, columns, new ArrayList<>()));
        streamStep.stream(GenericQueryResultInfoBuilder.builder()
                .setQueryType(GenericQueryResultInfo.QueryType.CREATE_TABLE)
                .setAffectedRowCount(0)
                .build());
    }
}
