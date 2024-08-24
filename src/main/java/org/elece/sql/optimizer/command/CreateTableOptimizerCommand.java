package org.elece.sql.optimizer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.parser.statement.CreateTableStatement;

import java.util.concurrent.atomic.AtomicInteger;

public class CreateTableOptimizerCommand implements IOptimizerCommand<CreateTableStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, CreateTableStatement statement) {
        AtomicInteger idGenerator = new AtomicInteger(1);
        for (Column column : statement.getColumns()) {
            column.setId(idGenerator.getAndIncrement());
        }
    }
}
