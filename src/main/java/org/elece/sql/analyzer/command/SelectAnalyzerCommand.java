package org.elece.sql.analyzer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Optional;

public class SelectAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        SelectStatement selectStatement = (SelectStatement) statement;

        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), selectStatement.getFrom());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        Collection collection = optionalCollection.get();

        for (Expression column : selectStatement.getColumns()) {
            if (column instanceof WildcardExpression) {
                continue;
            }
            analyzeExpression(collection, null, column);
        }

        analyzeWhere(collection, selectStatement.getWhere());

        for (Expression orderBy : selectStatement.getOrderBy()) {
            analyzeExpression(collection, null, orderBy);
        }
    }
}
