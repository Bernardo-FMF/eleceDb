package org.elece.sql.optimizer;

import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.statement.Statement;

public interface ISqlOptimizer {
    void optimize(IContext<String, TableMetadata> context, Statement statement);
}
