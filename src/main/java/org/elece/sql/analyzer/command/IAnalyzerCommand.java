package org.elece.sql.analyzer.command;

import org.elece.sql.analyzer.error.AnalyzerException;
import org.elece.sql.db.IContext;
import org.elece.sql.db.Schema;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.Statement;

public interface IAnalyzerCommand {
    void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException;

    default void analyzeExpression(Schema schema, SqlType sqlType, Expression expression) throws AnalyzerException {
        // TODO
    }

    default void analyzeWhere(Schema schema, Expression expression) throws AnalyzerException {
        // TODO
    }

    default void analyzeAssignment(TableMetadata table, Assignment assignment, Boolean allowIdentifiers) throws AnalyzerException {
        // TODO
    }
}
