package org.elece.query;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.sql.parser.statement.CreateTableStatement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CreateTableQueryExecutor implements QueryExecutor {
    private final String tableName;
    private final List<Column> columns;

    public CreateTableQueryExecutor(CreateTableStatement statement) {
        this.tableName = statement.getName();
        this.columns = statement.getColumns();
    }

    @Override
    public int execute(SchemaManager schemaManager) throws
                                                    SchemaException,
                                                    IOException,
                                                    BTreeException,
                                                    SerializationException,
                                                    StorageException,
                                                    DeserializationException,
                                                    DbException,
                                                    ExecutionException,
                                                    InterruptedException {
        schemaManager.createTable(new Table(tableName, columns, new ArrayList<>()));
        return 0;
    }
}
