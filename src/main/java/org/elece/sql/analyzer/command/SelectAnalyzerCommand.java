package org.elece.sql.analyzer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.statement.SelectStatement;

import java.util.Optional;

public class SelectAnalyzerCommand implements IAnalyzerCommand<SelectStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, SelectStatement statement) throws AnalyzerException {
        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), statement.getFrom());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        Collection collection = optionalCollection.get();

        for (Expression column : statement.getColumns()) {
            if (column instanceof WildcardExpression) {
                continue;
            }
            analyzeExpression(collection, null, column);
        }

        analyzeWhere(collection, statement.getWhere());

        for (Expression orderBy : statement.getOrderBy()) {
            analyzeExpression(collection, null, orderBy);
        }
    }
}
