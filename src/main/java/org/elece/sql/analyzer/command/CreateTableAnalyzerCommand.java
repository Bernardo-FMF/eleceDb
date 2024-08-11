package org.elece.sql.analyzer.command;

import org.elece.sql.error.AnalyzerException;
import org.elece.sql.db.Db;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.expression.internal.Column;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.statement.CreateTableStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class CreateTableAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException {
        CreateTableStatement createTableStatement = (CreateTableStatement) statement;

        if (!Objects.isNull(context.findMetadata(createTableStatement.getName()))) {
            throw new AnalyzerException("Table already exists");
        }

        boolean hasPrimaryKey = false;
        Set<String> columnNames = new HashSet<>();

        for (Column column : createTableStatement.getColumns()) {
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
