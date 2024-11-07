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
import org.elece.query.path.NodeCollection;
import org.elece.query.plan.QueryPlan;
import org.elece.query.plan.builder.DeleteQueryPlanBuilder;
import org.elece.query.plan.builder.InsertQueryPlanBuilder;
import org.elece.query.plan.builder.SelectQueryPlanBuilder;
import org.elece.query.plan.builder.UpdateQueryPlanBuilder;
import org.elece.query.plan.step.filter.FieldFilterStep;
import org.elece.query.plan.step.operation.DeleteOperationStep;
import org.elece.query.plan.step.operation.InsertOperationStep;
import org.elece.query.plan.step.operation.UpdateOperationStep;
import org.elece.query.plan.step.order.CacheableOrderStep;
import org.elece.query.plan.step.scan.EqualityRowScanStep;
import org.elece.query.plan.step.scan.RangeRowScanStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.scan.SequentialScanStep;
import org.elece.query.plan.step.selector.AttributeSelectorStep;
import org.elece.query.plan.step.stream.OutputStreamStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.InsertTracerStep;
import org.elece.query.plan.step.tracer.SelectTracerStep;
import org.elece.query.plan.step.tracer.UpdateTracerStep;
import org.elece.query.plan.step.validator.InsertValidatorStep;
import org.elece.query.plan.step.value.InsertValueStep;
import org.elece.query.result.ScanInfo;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.OrderIdentifierExpression;
import org.elece.sql.parser.expression.internal.Order;
import org.elece.sql.parser.statement.*;
import org.elece.storage.file.FileHandlerPool;
import org.elece.thread.ClientBridge;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
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

    public void plan(Statement statement, ClientBridge clientBridge) throws SchemaException, BTreeException,
                                                                            SerializationException, IOException,
                                                                            ExecutionException, StorageException,
                                                                            InterruptedException,
                                                                            DeserializationException, DbException,
                                                                            QueryException, ParserException,
                                                                            ProtoException {
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

        Optional<QueryPlan> plan = switch (statement.getStatementType()) {
            case Select -> buildSelectQueryPlan((SelectStatement) statement, streamStep);
            case Insert -> buildInsertQueryPlan((InsertStatement) statement, streamStep);
            case Update -> buildUpdateQueryPlan((UpdateStatement) statement, streamStep);
            case Delete -> buildDeleteQueryPlan((DeleteStatement) statement, streamStep);
            default -> Optional.empty();
        };

        if (plan.isEmpty()) {
            throw new QueryException(DbError.INVALID_QUERY_ERROR, "Invalid query");
        }

        plan.get().execute();
    }

    private <V extends Comparable<V>> Optional<QueryPlan> buildDeleteQueryPlan(DeleteStatement statement,
                                                                               StreamStep streamStep) throws
                                                                                                      QueryException,
                                                                                                      SchemaException,
                                                                                                      StorageException,
                                                                                                      BTreeException {
        Optional<Table> possibleTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (possibleTable.isEmpty()) {
            return Optional.empty();
        }
        Table table = possibleTable.get();

        DeleteQueryPlanBuilder builder = DeleteQueryPlanBuilder.builder();
        builder.setStreamStep(streamStep);

        NodeCollection nodeCollection = ScanQueryPlanGenerator.buildNodeCollection(table, statement.getWhere());
        if (nodeCollection.isEmpty()) {
            return Optional.empty();
        }

        Long scanId = -1L;
        List<Column> mainScans = new ArrayList<>();
        List<Column> secondaryScans = new ArrayList<>();
        for (IndexPath indexPath : nodeCollection.getIndexPaths()) {
            Optional<DefaultPathNode> possibleMainPath = ScanQueryPlanGenerator.findMainPath(indexPath);
            ScanStep scanStep;
            if (possibleMainPath.isEmpty()) {
                scanStep = new SequentialScanStep(table, columnIndexManagerProvider, databaseStorageManager);
                mainScans.add(SchemaSearcher.findClusterColumn(table));
            } else {
                DefaultPathNode mainPath = possibleMainPath.get();

                Optional<Column> column = SchemaSearcher.findColumn(table, mainPath.getColumnName());
                if (column.isEmpty()) {
                    return Optional.empty();
                }

                if (mainPath.getValueComparator() instanceof EqualityComparator<?> equalityComparator) {
                    scanStep = new EqualityRowScanStep<>(table, column.get(), (EqualityComparator<V>) equalityComparator, columnIndexManagerProvider, databaseStorageManager);
                } else {
                    scanStep = new RangeRowScanStep(table, column.get(), (NumberRangeComparator) mainPath.getValueComparator(), Order.DEFAULT_ORDER, columnIndexManagerProvider, databaseStorageManager);
                }
                mainScans.add(column.get());
            }
            builder.addScanStep(scanStep);

            if (scanId == -1L) {
                scanId = scanStep.getScanId();
            }

            Set<DefaultPathNode> secondaryPaths = possibleMainPath
                    .map(defaultPathNode -> indexPath.getNodePaths().stream().filter(pathNode -> !Objects.equals(pathNode, defaultPathNode)).collect(Collectors.toSet()))
                    .orElseGet(indexPath::getNodePaths);

            for (DefaultPathNode secondaryPath : secondaryPaths) {
                Optional<Column> column = SchemaSearcher.findColumn(table, secondaryPath.getColumnName());
                if (column.isPresent()) {
                    builder = builder.addFilterStep(new FieldFilterStep<>(table, column.get(), (ValueComparator<V>) secondaryPath.getValueComparator(), serializerRegistry, scanStep.getScanId()));
                    secondaryScans.add(column.get());
                }
            }
        }

        builder.setOperationStep(new DeleteOperationStep(table, columnIndexManagerProvider, databaseStorageManager))
                .setTracerStep(new UpdateTracerStep(table, new ScanInfo(mainScans, secondaryScans)));

        return Optional.of(builder.build());
    }

    private <V extends Comparable<V>> Optional<QueryPlan> buildUpdateQueryPlan(UpdateStatement statement,
                                                                               StreamStep streamStep) throws
                                                                                                      SchemaException,
                                                                                                      StorageException,
                                                                                                      QueryException,
                                                                                                      BTreeException {
        Optional<Table> possibleTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (possibleTable.isEmpty()) {
            return Optional.empty();
        }
        Table table = possibleTable.get();

        UpdateQueryPlanBuilder builder = UpdateQueryPlanBuilder.builder();
        builder.setStreamStep(streamStep);

        NodeCollection nodeCollection = ScanQueryPlanGenerator.buildNodeCollection(table, statement.getWhere());
        if (nodeCollection.isEmpty()) {
            return Optional.empty();
        }

        Long scanId = -1L;
        List<Column> mainScans = new ArrayList<>();
        List<Column> secondaryScans = new ArrayList<>();
        for (IndexPath indexPath : nodeCollection.getIndexPaths()) {
            Optional<DefaultPathNode> possibleMainPath = ScanQueryPlanGenerator.findMainPath(indexPath);
            ScanStep scanStep;
            if (possibleMainPath.isEmpty()) {
                scanStep = new SequentialScanStep(table, columnIndexManagerProvider, databaseStorageManager);
                mainScans.add(SchemaSearcher.findClusterColumn(table));
            } else {
                DefaultPathNode mainPath = possibleMainPath.get();
                if (mainPath.getValueComparator() instanceof EqualityComparator<?> equalityComparator) {
                    scanStep = new EqualityRowScanStep<>(table, SchemaSearcher.findColumn(table, mainPath.getColumnName()).get(), (EqualityComparator<V>) equalityComparator, columnIndexManagerProvider, databaseStorageManager);
                } else {
                    scanStep = new RangeRowScanStep(table, SchemaSearcher.findColumn(table, mainPath.getColumnName()).get(), (NumberRangeComparator) mainPath.getValueComparator(), Order.DEFAULT_ORDER, columnIndexManagerProvider, databaseStorageManager);
                }
                mainScans.add(SchemaSearcher.findColumn(table, mainPath.getColumnName()).get());
            }
            builder.addScanStep(scanStep);

            if (scanId == -1L) {
                scanId = scanStep.getScanId();
            }

            Set<DefaultPathNode> secondaryPaths = possibleMainPath
                    .map(defaultPathNode -> indexPath.getNodePaths().stream().filter(pathNode -> !Objects.equals(pathNode, defaultPathNode)).collect(Collectors.toSet()))
                    .orElseGet(indexPath::getNodePaths);

            for (DefaultPathNode secondaryPath : secondaryPaths) {
                Optional<Column> column = SchemaSearcher.findColumn(table, secondaryPath.getColumnName());
                if (column.isPresent()) {
                    builder = builder.addFilterStep(new FieldFilterStep<>(table, column.get(), (ValueComparator<V>) secondaryPath.getValueComparator(), serializerRegistry, scanStep.getScanId()));
                    secondaryScans.add(column.get());
                }
            }
        }

        builder.setOperationStep(new UpdateOperationStep(table, statement.getColumns(), databaseStorageManager, columnIndexManagerProvider, serializerRegistry))
                .setTracerStep(new UpdateTracerStep(table, new ScanInfo(mainScans, secondaryScans)));

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
                .setTracerStep(new InsertTracerStep(table));

        return Optional.of(builder.build());
    }

    private <V extends Comparable<V>> Optional<QueryPlan> buildSelectQueryPlan(SelectStatement statement,
                                                                               StreamStep streamStep) throws
                                                                                                      QueryException,
                                                                                                      SchemaException,
                                                                                                      StorageException,
                                                                                                      BTreeException {
        Optional<Table> possibleTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (possibleTable.isEmpty()) {
            return Optional.empty();
        }
        Table table = possibleTable.get();
        Order order = Objects.isNull(statement.getOrderBy()) ? Order.DEFAULT_ORDER : ((OrderIdentifierExpression) statement.getOrderBy().getFirst()).getOrder();

        List<Column> selectedColumns = statement.getColumns().stream()
                .map(IdentifierExpression.class::cast)
                .map(columnExpression -> SchemaSearcher.findColumn(table, columnExpression.getName()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();

        SelectQueryPlanBuilder builder = SelectQueryPlanBuilder.builder();
        builder.setStreamStep(streamStep);

        NodeCollection nodeCollection = ScanQueryPlanGenerator.buildNodeCollection(table, statement.getWhere());
        if (nodeCollection.isEmpty()) {
            return Optional.empty();
        }

        Long scanId = -1L;
        List<Column> mainScans = new ArrayList<>();
        List<Column> secondaryScans = new ArrayList<>();
        for (IndexPath indexPath : nodeCollection.getIndexPaths()) {
            Optional<DefaultPathNode> possibleMainPath = ScanQueryPlanGenerator.findMainPath(indexPath);
            ScanStep scanStep;
            if (possibleMainPath.isEmpty()) {
                scanStep = new SequentialScanStep(table, columnIndexManagerProvider, databaseStorageManager);
                mainScans.add(SchemaSearcher.findClusterColumn(table));
            } else {
                DefaultPathNode mainPath = possibleMainPath.get();
                if (mainPath.getValueComparator() instanceof EqualityComparator<?> equalityComparator) {
                    scanStep = new EqualityRowScanStep<>(table, SchemaSearcher.findColumn(table, mainPath.getColumnName()).get(), (EqualityComparator<V>) equalityComparator, columnIndexManagerProvider, databaseStorageManager);
                } else {
                    scanStep = new RangeRowScanStep(table, SchemaSearcher.findColumn(table, mainPath.getColumnName()).get(), (NumberRangeComparator) mainPath.getValueComparator(), order, columnIndexManagerProvider, databaseStorageManager);
                }
                mainScans.add(SchemaSearcher.findColumn(table, mainPath.getColumnName()).get());
            }
            builder.addScanStep(scanStep);

            if (scanId == -1L) {
                scanId = scanStep.getScanId();
            }

            Set<DefaultPathNode> secondaryPaths = possibleMainPath
                    .map(defaultPathNode -> indexPath.getNodePaths().stream().filter(pathNode -> !Objects.equals(pathNode, defaultPathNode)).collect(Collectors.toSet()))
                    .orElseGet(indexPath::getNodePaths);

            for (DefaultPathNode secondaryPath : secondaryPaths) {
                Optional<Column> column = SchemaSearcher.findColumn(table, secondaryPath.getColumnName());
                if (column.isPresent()) {
                    builder = builder.addFilterStep(new FieldFilterStep<>(table, column.get(), (ValueComparator<V>) secondaryPath.getValueComparator(), serializerRegistry, scanStep.getScanId()));
                    secondaryScans.add(column.get());
                }
            }
        }

        builder.setSelectorStep(new AttributeSelectorStep(table, selectedColumns))
                .setTracerStep(new SelectTracerStep(selectedColumns, table, new ScanInfo(mainScans, secondaryScans)));

        if (!Objects.isNull(statement.getOrderBy())) {
            OrderIdentifierExpression orderBy = (OrderIdentifierExpression) statement.getOrderBy().getFirst();
            Optional<Column> column = SchemaSearcher.findColumn(table, orderBy.getName());
            if (column.isPresent()) {
                builder.setOrderStep(new CacheableOrderStep<>(order, selectedColumns, column.get(), scanId, fileHandlerPool, serializerRegistry, dbConfig));
            }
        }

        return Optional.of(builder.build());
    }
}
