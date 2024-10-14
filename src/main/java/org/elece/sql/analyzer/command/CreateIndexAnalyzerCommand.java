package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.*;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.statement.CreateIndexStatement;

import java.util.List;
import java.util.Optional;

public class CreateIndexAnalyzerCommand implements AnalyzerCommand<CreateIndexStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, CreateIndexStatement statement) throws AnalyzerException {
        if (!statement.getUnique()) {
            throw new AnalyzerException(new IndexNotUniqueError(statement.getName()));
        }

        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(new TableNotPresentError(statement.getTable()));
        }

        Table table = optionalTable.get();

        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            if (index.getName().equals(statement.getName())) {
                throw new AnalyzerException(new IndexAlreadyExistsError(index.getName()));
            }
            if (index.getColumnName().equals(statement.getColumn())) {
                throw new AnalyzerException(new ColumnIsAlreadyIndexedError(statement.getColumn()));
            }
        }

        Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, statement.getColumn());
        if (optionalColumn.isEmpty()) {
            throw new AnalyzerException(new ColumnNotPresentError(statement.getColumn(), statement.getTable()));
        }

        Column column = optionalColumn.get();
        if (!column.getConstraints().contains(SqlConstraint.Unique)) {
            throw new AnalyzerException(new ColumnIsNotUniqueError(column.getName(), table.getName()));
        }
    }
}
