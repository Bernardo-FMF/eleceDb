package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.statement.CreateTableStatement;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class CreateTableAnalyzerCommand implements IAnalyzerCommand<CreateTableStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, CreateTableStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getName());
        if (optionalTable.isPresent()) {
            throw new AnalyzerException("Table already exists");
        }

        boolean hasPrimaryKey = false;
        Set<String> columnNames = new HashSet<>();

        for (Column column : statement.getColumns()) {
            if (columnNames.contains(column.getName())) {
                throw new AnalyzerException("Duplicate columns");
            } else {
                columnNames.add(column.getName());
            }

            if (Db.ROW_ID.equals(column.getName())) {
                throw new AnalyzerException("");
            }

            if (column.getConstraints().contains(SqlConstraint.PrimaryKey)) {
                if (hasPrimaryKey) {
                    throw new AnalyzerException("");
                }
                if (!column.getSqlType().getConstraints().contains(SqlConstraint.PrimaryKey)) {
                    throw new AnalyzerException("");
                }
                hasPrimaryKey = true;
            }

            if (column.getConstraints().contains(SqlConstraint.Unique) && !column.getSqlType().getConstraints().contains(SqlConstraint.Unique)) {
                throw new AnalyzerException("");
            }
        }
    }
}
