package org.elece.query;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.proto.TcpException;
import org.elece.exception.schema.SchemaException;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.result.GenericQueryResultInfo;
import org.elece.query.result.builder.GenericQueryResultInfoBuilder;
import org.elece.sql.parser.statement.CreateDbStatement;

import java.io.IOException;

public class CreateDbQueryExecutor implements QueryExecutor {
    private final String db;
    private final StreamStep streamStep;

    public CreateDbQueryExecutor(CreateDbStatement statement, StreamStep streamStep) {
        this.db = statement.getDb();
        this.streamStep = streamStep;
    }

    @Override
    public void execute(SchemaManager schemaManager) throws SchemaException, IOException, TcpException {
        schemaManager.createSchema(db);
        streamStep.stream(GenericQueryResultInfoBuilder.builder()
                .setQueryType(GenericQueryResultInfo.QueryType.CREATE_DB)
                .setAffectedRowCount(0)
                .build());
    }
}
