package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.statement.SelectStatement;

import java.util.Optional;

public class SelectAnalyzerCommand implements IAnalyzerCommand<SelectStatement> {
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
            analyzeExpression(table, null, column);
        }

        analyzeWhere(table, statement.getWhere());

        for (Expression orderBy : statement.getOrderBy()) {
            analyzeExpression(table, null, orderBy);
        }
    }
}
