package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.MultipleOrderByExpressionsError;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.statement.SelectStatement;

import java.util.Optional;

public class SelectAnalyzerCommand implements AnalyzerCommand<SelectStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, SelectStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(new TableNotPresentError(statement.getFrom()));
        }

        Table table = optionalTable.get();

        for (Expression column : statement.getColumns()) {
            if (column instanceof WildcardExpression) {
                continue;
            }
            analyzeExpression(new ExpressionContext(table, null), column);
        }

        analyzeWhere(table, statement.getWhere());

        if (statement.getOrderBy().size() > 1) {
            throw new AnalyzerException(new MultipleOrderByExpressionsError());
        }

        for (Expression orderBy : statement.getOrderBy()) {
            analyzeExpression(new ExpressionContext(table, null), orderBy);
        }
    }
}
