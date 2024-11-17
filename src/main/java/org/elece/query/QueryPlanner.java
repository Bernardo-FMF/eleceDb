package org.elece.query;

import org.elece.config.DbConfig;
import org.elece.db.DatabaseStorageManager;
import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.query.comparator.EqualityComparator;
import org.elece.query.comparator.NumberRangeComparator;
import org.elece.query.comparator.ValueComparator;
import org.elece.query.executor.*;
import org.elece.query.path.DefaultPathNode;
import org.elece.query.path.IndexPath;
import org.elece.query.path.IndexPathFinder;
import org.elece.query.path.NodeCollection;
import org.elece.query.plan.QueryPlan;
import org.elece.query.plan.builder.DeleteQueryPlanBuilder;
import org.elece.query.plan.builder.InsertQueryPlanBuilder;
import org.elece.query.plan.builder.SelectQueryPlanBuilder;
import org.elece.query.plan.builder.UpdateQueryPlanBuilder;
import org.elece.query.plan.step.deserializer.RowDeserializerStep;
import org.elece.query.plan.step.filter.FieldFilterStep;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.operation.DeleteOperationStep;
import org.elece.query.plan.step.operation.InsertOperationStep;
import org.elece.query.plan.step.operation.UpdateOperationStep;
import org.elece.query.plan.step.order.CacheableOrderStep;
import org.elece.query.plan.step.scan.*;
import org.elece.query.plan.step.selector.AttributeSelectorStep;
import org.elece.query.plan.step.stream.OutputStreamStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.GenericTracerStep;
import org.elece.query.plan.step.tracer.SelectInitialTracerStep;
import org.elece.query.plan.step.validator.InsertValidatorStep;
import org.elece.query.plan.step.value.InsertValueStep;
import org.elece.query.result.GenericQueryResultInfo;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.OrderIdentifierExpression;
import org.elece.sql.parser.expression.internal.Order;
import org.elece.sql.parser.statement.*;
import org.elece.storage.file.FileHandlerPool;
import org.elece.thread.ClientInterface;

import java.util.*;
import java.util.stream.Collectors;

public class QueryPlanner {
    private final SchemaManager schemaManager;
    private final DatabaseStorageManager databaseStorageManager;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final SerializerRegistry serializerRegistry;
    private final FileHandlerPool fileHandlerPool;
    private final DbConfig dbConfig;

    public QueryPlanner(SchemaManager schemaManager, DatabaseStorageManager databaseStorageManager,
                        ColumnIndexManagerProvider columnIndexManagerProvider, SerializerRegistry serializerRegistry,
                        FileHandlerPool fileHandlerPool, DbConfig dbConfig) {
        this.schemaManager = schemaManager;
        this.databaseStorageManager = databaseStorageManager;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.serializerRegistry = serializerRegistry;
        this.fileHandlerPool = fileHandlerPool;
        this.dbConfig = dbConfig;
    }

    public void plan(Statement statement, ClientInterface clientInterface) throws SchemaException, BTreeException,
                                                                                  SerializationException,
                                                                                  StorageException,
                                                                                  DeserializationException, DbException,
                                                                                  QueryException, ParserException,
                                                                                  ProtoException,
                                                                                  InterruptedTaskException,
                                                                                  FileChannelException {
        final StreamStep streamStep = new OutputStreamStep(clientInterface);
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

        Optional<QueryPlan> plan = switch (statement.getStatementType()) {
            case Select -> buildSelectQueryPlan((SelectStatement) statement, streamStep);
            case Insert -> buildInsertQueryPlan((InsertStatement) statement, streamStep);
            case Update -> buildUpdateQueryPlan((UpdateStatement) statement, streamStep);
            case Delete -> buildDeleteQueryPlan((DeleteStatement) statement, streamStep);
            default -> Optional.empty();
        };

        if (plan.isEmpty()) {
            throw new QueryException(DbError.INVALID_QUERY_ERROR, "Failed to build an executable query plan");
        }

        plan.get().execute();
    }

    private Optional<QueryPlan> buildDeleteQueryPlan(DeleteStatement statement,
                                                     StreamStep streamStep) throws QueryException, SchemaException,
                                                                                   StorageException, BTreeException,
                                                                                   InterruptedTaskException,
                                                                                   FileChannelException {
        Optional<Table> possibleTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (possibleTable.isEmpty()) {
            return Optional.empty();
        }
        Table table = possibleTable.get();

        NodeCollection nodeCollection = buildNodeCollection(table, statement.getWhere());
        QueryContext queryContext = new QueryContext();
        findScanPaths(queryContext, table, nodeCollection, Order.DEFAULT_ORDER);

        DeleteQueryPlanBuilder builder = DeleteQueryPlanBuilder.builder();
        for (ScanStep scanStep : queryContext.getScanSteps()) {
            builder.addScanStep(scanStep);
        }
        for (FilterStep filterStep : queryContext.getFilterSteps()) {
            builder.addFilterStep(filterStep);
        }
        builder.setOperationStep(new DeleteOperationStep(table, columnIndexManagerProvider, databaseStorageManager))
                .setTracerStep(new GenericTracerStep<>(GenericQueryResultInfo.QueryType.DELETE))
                .setStreamStep(streamStep);

        return Optional.of(builder.build());
    }

    private Optional<QueryPlan> buildUpdateQueryPlan(UpdateStatement statement,
                                                     StreamStep streamStep) throws SchemaException, StorageException,
                                                                                   QueryException, BTreeException,
                                                                                   InterruptedTaskException,
                                                                                   FileChannelException {
        Optional<Table> possibleTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (possibleTable.isEmpty()) {
            return Optional.empty();
        }
        Table table = possibleTable.get();

        NodeCollection nodeCollection = buildNodeCollection(table, statement.getWhere());
        QueryContext queryContext = new QueryContext();
        findScanPaths(queryContext, table, nodeCollection, Order.DEFAULT_ORDER);

        UpdateQueryPlanBuilder builder = UpdateQueryPlanBuilder.builder();
        for (ScanStep scanStep : queryContext.getScanSteps()) {
            builder.addScanStep(scanStep);
        }
        for (FilterStep filterStep : queryContext.getFilterSteps()) {
            builder.addFilterStep(filterStep);
        }
        builder.setOperationStep(new UpdateOperationStep(table, statement.getColumns(), databaseStorageManager, columnIndexManagerProvider, serializerRegistry))
                .setTracerStep(new GenericTracerStep<>(GenericQueryResultInfo.QueryType.UPDATE))
                .setStreamStep(streamStep);

        return Optional.of(builder.build());
    }

    private Optional<QueryPlan> buildInsertQueryPlan(InsertStatement statement, StreamStep streamStep) {
        Optional<Table> possibleTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (possibleTable.isEmpty()) {
            return Optional.empty();
        }
        Table table = possibleTable.get();

        InsertQueryPlanBuilder builder = InsertQueryPlanBuilder.builder();
        builder.setStreamStep(streamStep)
                .setValueStep(new InsertValueStep(table, statement.getValues(), columnIndexManagerProvider, serializerRegistry))
                .setValidatorStep(new InsertValidatorStep(table, columnIndexManagerProvider))
                .setOperationStep(new InsertOperationStep(table, columnIndexManagerProvider, databaseStorageManager))
                .setTracerStep(new GenericTracerStep<>(GenericQueryResultInfo.QueryType.INSERT));

        return Optional.of(builder.build());
    }

    private Optional<QueryPlan> buildSelectQueryPlan(SelectStatement statement,
                                                     StreamStep streamStep) throws QueryException, SchemaException,
                                                                                   StorageException, BTreeException,
                                                                                   InterruptedTaskException,
                                                                                   FileChannelException {
        Optional<Table> possibleTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (possibleTable.isEmpty()) {
            return Optional.empty();
        }
        Table table = possibleTable.get();
        Order order = Objects.isNull(statement.getOrderBy()) || statement.getOrderBy().isEmpty() ? Order.DEFAULT_ORDER : ((OrderIdentifierExpression) statement.getOrderBy().getFirst()).getOrder();

        List<Column> selectedColumns = statement.getColumns().stream()
                .map(IdentifierExpression.class::cast)
                .map(columnExpression -> SchemaSearcher.findColumn(table, columnExpression.getName()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();

        NodeCollection nodeCollection = buildNodeCollection(table, statement.getWhere());
        QueryContext queryContext = new QueryContext();
        findScanPaths(queryContext, table, nodeCollection, order);

        SelectQueryPlanBuilder builder = SelectQueryPlanBuilder.builder();
        for (ScanStep scanStep : queryContext.getScanSteps()) {
            builder.addScanStep(scanStep);
        }
        for (FilterStep filterStep : queryContext.getFilterSteps()) {
            builder.addFilterStep(filterStep);
        }
        builder.setSelectorStep(new AttributeSelectorStep(table, selectedColumns))
                .setInitialTracerStep(new SelectInitialTracerStep(selectedColumns))
                .setEndTracerStep(new GenericTracerStep<>(GenericQueryResultInfo.QueryType.SELECT_END))
                .setDeserializerStep(new RowDeserializerStep(serializerRegistry, selectedColumns))
                .setStreamStep(streamStep);

        if (!Objects.isNull(statement.getOrderBy()) && !statement.getOrderBy().isEmpty()) {
            OrderIdentifierExpression orderBy = (OrderIdentifierExpression) statement.getOrderBy().getFirst();
            Optional<Column> column = SchemaSearcher.findColumn(table, orderBy.getName());
            column.ifPresent(value -> builder.setOrderStep(new CacheableOrderStep<>(order, selectedColumns, value, queryContext.getScanSteps().getFirst().getScanId(), fileHandlerPool, serializerRegistry, dbConfig)));
        }

        return Optional.of(builder.build());
    }

    private <V extends Comparable<V>> void findScanPaths(QueryContext queryContext, Table table,
                                                         NodeCollection nodeCollection, Order order) throws
                                                                                                     SchemaException,
                                                                                                     InterruptedTaskException,
                                                                                                     StorageException,
                                                                                                     FileChannelException,
                                                                                                     BTreeException {
        if (nodeCollection.isEmpty()) {
            ScanStep scanStep = new SequentialScanStep(table, columnIndexManagerProvider, databaseStorageManager);
            queryContext.getScanInfo().addMainScan(SchemaSearcher.findClusterColumn(table));
            queryContext.addScanStep(scanStep);
            return;
        }
        for (IndexPath indexPath : nodeCollection.getIndexPaths()) {
            Optional<DefaultPathNode> possibleMainPath = findMainPath(indexPath);
            ScanStep scanStep;
            if (possibleMainPath.isEmpty()) {
                scanStep = new SequentialScanStep(table, columnIndexManagerProvider, databaseStorageManager);
                queryContext.getScanInfo().addMainScan(SchemaSearcher.findClusterColumn(table));
            } else {
                DefaultPathNode mainPath = possibleMainPath.get();
                Optional<Column> column = SchemaSearcher.findColumn(table, mainPath.getColumnName());
                if (column.isEmpty()) {
                    continue;
                }

                if (mainPath.getValueComparator() instanceof EqualityComparator<?> equalityComparator) {
                    if (equalityComparator.shouldBeEqual()) {
                        scanStep = new EqualityRowScanStep<>(table, column.get(), (EqualityComparator<V>) equalityComparator, columnIndexManagerProvider, databaseStorageManager);
                    } else {
                        scanStep = new InequalityRowScanStep<>(table, column.get(), (EqualityComparator<V>) equalityComparator, columnIndexManagerProvider, databaseStorageManager);
                    }
                } else {
                    scanStep = new RangeRowScanStep(table, column.get(), (NumberRangeComparator) mainPath.getValueComparator(), order, columnIndexManagerProvider, databaseStorageManager);
                }
                queryContext.getScanInfo().addMainScan(column.get());
            }
            queryContext.addScanStep(scanStep);

            findSecondaryScanPaths(queryContext, table, indexPath, possibleMainPath, scanStep);
        }
    }

    private <V extends Comparable<V>> void findSecondaryScanPaths(QueryContext queryContext, Table table,
                                                                  IndexPath indexPath,
                                                                  Optional<DefaultPathNode> possibleMainPath,
                                                                  ScanStep scanStep) {
        Set<DefaultPathNode> secondaryPaths = possibleMainPath
                .map(defaultPathNode -> indexPath.getNodePaths().stream().filter(pathNode -> !Objects.equals(pathNode, defaultPathNode)).collect(Collectors.toSet()))
                .orElseGet(indexPath::getNodePaths);

        for (DefaultPathNode secondaryPath : secondaryPaths) {
            Optional<Column> column = SchemaSearcher.findColumn(table, secondaryPath.getColumnName());
            if (column.isPresent()) {
                queryContext.addFilterStep(new FieldFilterStep<>(table, column.get(), (ValueComparator<V>) secondaryPath.getValueComparator(), serializerRegistry, scanStep.getScanId()));
                queryContext.getScanInfo().addSecondaryFilterScan(column.get());
            }
        }
    }

    private NodeCollection buildNodeCollection(Table table, Expression filter) throws QueryException {
        if (Objects.isNull(filter)) {
            return new NodeCollection();
        }
        return filter.accept(new IndexPathFinder(table));
    }

    private Optional<DefaultPathNode> findMainPath(IndexPath indexPath) {
        Queue<DefaultPathNode> nodePaths = indexPath.buildNodePathsQueue();
        if (nodePaths.isEmpty()) {
            return Optional.empty();
        }
        DefaultPathNode polledNode = nodePaths.poll();
        if (polledNode.getIndexType() == DefaultPathNode.IndexType.NON_INDEXED) {
            return Optional.empty();
        }
        return Optional.of(polledNode);
    }
}
