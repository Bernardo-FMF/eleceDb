package org.elece.sql.analyzer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Index;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.statement.CreateIndexStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.List;
import java.util.Optional;

public class CreateIndexAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        CreateIndexStatement createIndexStatement = (CreateIndexStatement) statement;

        if (!createIndexStatement.getUnique()) {
            throw new AnalyzerException("");
        }

        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), createIndexStatement.getTable());

        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        Collection collection = optionalCollection.get();

        List<Index> indexes = collection.getIndexes();
        for (Index index : indexes) {
            if (index.getName().equals(createIndexStatement.getName())) {
                throw new AnalyzerException("");
            }
            if (index.getColumnName().equals(createIndexStatement.getColumn())) {
                throw new AnalyzerException("");
            }
        }

        Optional<Column> optionalColumn = SchemaSearcher.findColumn(collection, createIndexStatement.getColumn());
        if (optionalColumn.isEmpty()) {
            throw new AnalyzerException("");
        }

        Column column = optionalColumn.get();
        if (column.getConstraints().contains(SqlConstraint.Unique)) {
            throw new AnalyzerException("");
        }
    }
}
