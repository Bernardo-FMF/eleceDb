package org.elece.sql.analyzer.command;

import org.elece.sql.error.AnalyzerException;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Objects;

public class SelectAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException {
        SelectStatement selectStatement = (SelectStatement) statement;

        TableMetadata table = context.findMetadata(selectStatement.getFrom());

        if (Objects.isNull(table)) {
            throw new AnalyzerException("");
        }

        for (Expression column : selectStatement.getColumns()) {
            if (column instanceof WildcardExpression) {
                continue;
            }
            analyzeExpression(table.schema(), null, column);
        }

        analyzeWhere(table.schema(), selectStatement.getWhere());

        for (Expression orderBy : selectStatement.getOrderBy()) {
            analyzeExpression(table.schema(), null, orderBy);
        }
    }
}
