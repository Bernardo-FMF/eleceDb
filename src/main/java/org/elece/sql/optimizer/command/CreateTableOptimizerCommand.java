package org.elece.sql.optimizer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.builder.ColumnBuilder;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.CreateTableStatement;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class CreateTableOptimizerCommand implements IOptimizerCommand<CreateTableStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, CreateTableStatement statement) {
        AtomicInteger idGenerator = new AtomicInteger(1);

        Column clusterColumn = ColumnBuilder.builder()
                .setName(CLUSTER_ID)
                .setSqlType(SqlType.intType)
                .setConstraints(List.of(SqlConstraint.Unique))
                .build();
        clusterColumn.setId(idGenerator.getAndIncrement());

        statement.getColumns().addFirst(clusterColumn);

        for (Column column : statement.getColumns()) {
            column.setId(idGenerator.getAndIncrement());
        }
    }
}
