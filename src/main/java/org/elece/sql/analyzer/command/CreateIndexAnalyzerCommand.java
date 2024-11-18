package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Table;
import org.elece.exception.AnalyzerException;
import org.elece.exception.DbError;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.statement.CreateIndexStatement;

import java.util.List;
import java.util.Optional;

public class CreateIndexAnalyzerCommand implements AnalyzerCommand<CreateIndexStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, CreateIndexStatement statement) throws AnalyzerException {
        if (!statement.getUnique()) {
            throw new AnalyzerException(DbError.INDEX_NOT_UNIQUE_ERROR, String.format("Index %s is not unique", statement.getName()));
        }

        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(DbError.TABLE_NOT_FOUND_ERROR, String.format("Table %s is not present in the database schema", statement.getTable()));
        }

        Table table = optionalTable.get();

        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            if (index.getName().equals(statement.getName())) {
                throw new AnalyzerException(DbError.INDEX_ALREADY_EXISTS_ERROR, String.format("Index %s already exists", index.getName()));
            }
            if (index.getColumnName().equals(statement.getColumn())) {
                throw new AnalyzerException(DbError.COLUMN_ALREADY_INDEXED_ERROR, String.format("Column %s is already indexed", index.getColumnName()));
            }
        }

        Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, statement.getColumn());
        if (optionalColumn.isEmpty()) {
            throw new AnalyzerException(DbError.COLUMN_NOT_FOUND_ERROR, String.format("Column %s is not present in the table %s", statement.getColumn(), statement.getTable()));
        }

        Column column = optionalColumn.get();
        if (!column.getSqlType().getConstraints().contains(SqlConstraint.UNIQUE)) {
            throw new AnalyzerException(DbError.COLUMN_NOT_UNIQUE_ERROR, String.format("Column %s of the table %s is not unique", column.getName(), table.getName()));
        }
    }
}
