package org.elece.query;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.schema.SchemaManager;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.proto.TcpException;
import org.elece.exception.query.QueryException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.storage.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.query.executor.*;
import org.elece.query.plan.QueryPlan;
import org.elece.query.plan.step.stream.OutputStreamStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.statement.*;
import org.elece.thread.ClientBridge;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class QueryPlanner {
    private final SchemaManager schemaManager;
    private final DatabaseStorageManager databaseStorageManager;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final SerializerRegistry serializerRegistry;

    public QueryPlanner(SchemaManager schemaManager, DatabaseStorageManager databaseStorageManager,
                        ColumnIndexManagerProvider columnIndexManagerProvider, SerializerRegistry serializerRegistry) {
        this.schemaManager = schemaManager;
        this.databaseStorageManager = databaseStorageManager;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.serializerRegistry = serializerRegistry;
    }

    public void plan(Statement statement, ClientBridge clientBridge) throws SchemaException, BTreeException,
                                                                            SerializationException, IOException,
                                                                            ExecutionException, StorageException,
                                                                            InterruptedException,
                                                                            DeserializationException, DbException,
                                                                            QueryException, ParserException,
                                                                            TcpException {
        final StreamStep streamStep = new OutputStreamStep(clientBridge);
        Optional<QueryExecutor> queryExecutor = switch (statement.getStatementType()) {
            case CreateDb -> Optional.of(new CreateDbQueryExecutor((CreateDbStatement) statement, streamStep));
            case CreateIndex -> Optional.of(new CreateIndexQueryExecutor((CreateIndexStatement) statement, streamStep));
            case CreateTable -> Optional.of(new CreateTableQueryExecutor((CreateTableStatement) statement, streamStep));
            case DropDb -> Optional.of(new DropDbQueryExecutor(streamStep));
            case DropTable -> Optional.of(new DropTableQueryExecutor((DropTableStatement) statement, streamStep));
            default -> Optional.empty();
        };

        if (queryExecutor.isPresent()) {
            queryExecutor.get().execute(schemaManager);
            return;
        }

        // TODO: implement query builders
        Optional<QueryPlan> plan = switch (statement.getStatementType()) {
            case Insert -> Optional.empty();
            case Select -> Optional.empty();
            case Update -> Optional.empty();
            case Delete -> Optional.empty();
            default -> Optional.empty();
        };

        if (plan.isEmpty()) {
            // TODO: fix exception
            throw new QueryException(null);
        }

    }
}
