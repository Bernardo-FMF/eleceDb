package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.AnalyzerException;
import org.elece.exception.DbError;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.statement.CreateTableStatement;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class CreateTableAnalyzerCommand implements AnalyzerCommand<CreateTableStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, CreateTableStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getName());
        if (optionalTable.isPresent()) {
            throw new AnalyzerException(DbError.TABLE_ALREADY_EXISTS_ERROR, String.format("Table %s already exists", statement.getName()));
        }

        boolean hasPrimaryKey = false;
        Set<String> columnNames = new HashSet<>();

        for (Column column : statement.getColumns()) {
            if (columnNames.contains(column.getName())) {
                throw new AnalyzerException(DbError.DUPLICATE_COLUMN_ERROR, String.format("Column %s is duplicated", column.getName()));
            } else {
                columnNames.add(column.getName());
            }

            if (CLUSTER_ID.equals(column.getName())) {
                throw new AnalyzerException(DbError.INVALID_COLUMN_ERROR, String.format("Column %s is invalid", CLUSTER_ID));
            }

            if (column.getConstraints().contains(SqlConstraint.PRIMARY_KEY)) {
                if (hasPrimaryKey) {
                    throw new AnalyzerException(DbError.MULTIPLE_PRIMARY_KEYS_ERROR, "Table definition contains multiple primary keys");
                }
                if (!column.getSqlType().getConstraints().contains(SqlConstraint.PRIMARY_KEY)) {
                    throw new AnalyzerException(DbError.INCOMPATIBLE_TYPE_FOR_PRIMARY_KEY_ERROR, String.format("Type %s used for column %s is not usable as primary key", column.getName(), column.getSqlType().getType()));
                }
                hasPrimaryKey = true;
            }

            if (column.getConstraints().contains(SqlConstraint.UNIQUE) && !column.getSqlType().getConstraints().contains(SqlConstraint.UNIQUE)) {
                throw new AnalyzerException(DbError.INCOMPATIBLE_TYPE_FOR_INDEX_ERROR, String.format("Type %s used for column %s is not usable for index", column.getName(), column.getSqlType().getType()));
            }
        }
    }
}
