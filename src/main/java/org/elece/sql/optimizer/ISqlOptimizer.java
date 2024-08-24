package org.elece.sql.optimizer;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.statement.Statement;

public interface ISqlOptimizer {
    void optimize(SchemaManager schemaManager, Statement statement) throws ParserException;
}
