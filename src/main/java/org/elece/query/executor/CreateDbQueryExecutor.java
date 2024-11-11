package org.elece.query.executor;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.ProtoException;
import org.elece.exception.SchemaException;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.result.GenericQueryResultInfo;
import org.elece.query.result.builder.GenericQueryResultInfoBuilder;
import org.elece.sql.parser.statement.CreateDbStatement;

public class CreateDbQueryExecutor implements QueryExecutor {
    private final String db;
    private final StreamStep streamStep;

    public CreateDbQueryExecutor(CreateDbStatement statement, StreamStep streamStep) {
        this.db = statement.getDb();
        this.streamStep = streamStep;
    }

    @Override
    public void execute(SchemaManager schemaManager) throws SchemaException, ProtoException {
        schemaManager.createSchema(db);
        streamStep.stream(GenericQueryResultInfoBuilder.builder()
                .setQueryType(GenericQueryResultInfo.QueryType.CREATE_DB)
                .setAffectedRowCount(0)
                .setMessage("Database created")
                .build());
    }
}
