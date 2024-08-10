package org.elece.sql.optimizer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.Column;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.statement.InsertStatement;
import org.elece.sql.parser.statement.Statement;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InsertOptimizerCommand implements IOptimizerCommand {
    @Override
    public void optimize(IContext<String, TableMetadata> context, Statement statement) throws ParserException {
        InsertStatement insertStatement = (InsertStatement) statement;

        insertStatement.setValues(optimizeExpressions(insertStatement.getValues()));

        TableMetadata table = context.findMetadata(insertStatement.getTable());
        if (insertStatement.getColumns().isEmpty()) {
            insertStatement.setColumns(table.getSchema().getColumns().stream().map(Column::getName).toList());
        }

        if (table.getSchema().getColumns().get(0).getName().equals(Db.ROW_ID)) {
            if (!insertStatement.getColumns().get(0).equals(Db.ROW_ID)) {
                ArrayList<String> newColumns = new ArrayList<>(insertStatement.getColumns());
                newColumns.add(0, Db.ROW_ID);
                insertStatement.setColumns(newColumns);
            }
            Long nextRowId = table.getNextRowId();
            ArrayList<Expression> newValues = new ArrayList<>(insertStatement.getValues());
            newValues.add(0, new ValueExpression<>(new SqlNumberValue(new BigInteger(String.valueOf(nextRowId)))));
            insertStatement.setValues(newValues);
        }

        List<Column> columns = table.getSchema().getColumns();
        for (int currentIndex = 0; currentIndex < columns.size(); currentIndex++) {
            Column currentColumn = columns.get(currentIndex);
            Integer sortedIndex = table.getSchema().findColumnIndex(currentColumn.getName());

            if (currentIndex != sortedIndex) {
                Collections.swap(columns, currentIndex, sortedIndex);
                Collections.swap(insertStatement.getValues(), currentIndex, sortedIndex);
            }
        }
    }
}
