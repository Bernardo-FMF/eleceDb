package org.elece.query;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.schema.SchemaException;
import org.elece.sql.parser.statement.CreateDbStatement;

import java.io.IOException;

public class CreateDbQueryExecutor implements QueryExecutor {
    private final String db;

    public CreateDbQueryExecutor(CreateDbStatement statement) {
        this.db = statement.getDb();
    }

    @Override
    public int execute(SchemaManager schemaManager) throws
                                                    SchemaException,
                                                    IOException {
        schemaManager.createSchema(db);
        return 0;
    }
}
