package org.elece.db.schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.elece.config.DbConfig;
import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.db.schema.model.builder.SchemaBuilder;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.schema.type.SchemaAlreadyExistsError;
import org.elece.exception.schema.type.SchemaDoesNotExistError;
import org.elece.exception.schema.type.SchemaPersistenceError;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.type.analyzer.ColumnNotPresentError;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.exception.storage.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.index.LockableIterator;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.utils.SerializationUtils;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class JsonSchemaManager implements SchemaManager {
    private Schema schema;
    private final DbConfig dbConfig;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final DatabaseStorageManager databaseStorageManager;
    private final Gson gson;
    private final AtomicInteger tableIndex;

    public JsonSchemaManager(DbConfig dbConfig, ColumnIndexManagerProvider columnIndexManagerProvider, DatabaseStorageManager databaseStorageManager) throws SchemaException {
        this.dbConfig = dbConfig;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.databaseStorageManager = databaseStorageManager;
        this.gson = new GsonBuilder().serializeNulls().create();
        loadSchema();

        tableIndex = new AtomicInteger(Objects.isNull(schema) || schema.getCollections().isEmpty() ? 0 : schema.getCollections().getLast().getId());
    }

    private void loadSchema() throws SchemaException {
        String schemePath = getSchemePath();
        if (!Files.exists(Path.of(schemePath))) {
            this.schema = null;
            return;
        }
        try {
            FileReader fileReader = new FileReader(schemePath);
            JsonReader jsonReader = new JsonReader(fileReader);
            this.schema = gson.fromJson(jsonReader, Schema.class);
            fileReader.close();
        } catch (IOException exception) {
            throw new SchemaException(new SchemaPersistenceError(exception.getMessage()));
        }
    }

    private String getSchemePath() {
        return Path.of(this.dbConfig.getBaseDbPath(), "schema.json").toString();
    }

    private void persistSchema() throws SchemaException {
        try {
            FileWriter fileWriter = new FileWriter(this.getSchemePath());
            gson.toJson(this.schema, fileWriter);
            fileWriter.close();
        } catch (IOException exception) {
            throw new SchemaException(new SchemaPersistenceError(exception.getMessage()));
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public synchronized void createSchema(String dbName) throws SchemaException {
        if (!Objects.isNull(schema)) {
            throw new SchemaException(new SchemaAlreadyExistsError());
        }

        this.schema = SchemaBuilder.builder()
                .setDbName(dbName)
                .setTables(new ArrayList<>())
                .build();
        persistSchema();
    }

    @Override
    public int deleteSchema() throws IOException, SchemaException, ExecutionException, InterruptedException, StorageException, DbException {
        validateSchemaExists();

        int totalRowCount = 0;
        for (Table table : schema.getCollections()) {
            totalRowCount += deleteTable(table.getName());
        }

        this.schema = null;

        persistSchema();

        return totalRowCount;
    }

    @Override
    public synchronized void createTable(Table table) throws SchemaException, StorageException {
        validateSchemaExists();

        int nextTableIndex = tableIndex.incrementAndGet();
        table.setId(nextTableIndex);

        for (Column column : table.getColumns()) {
            if (CLUSTER_ID.equals(column.getName())) {
                table.addIndex(new Index("cluster_index", CLUSTER_ID));
                columnIndexManagerProvider.getClusterIndexManager(table);
            } else if (column.isUnique()) {
                table.addIndex(new Index(String.format("col_index_%d", column.getId()), column.getName()));
                columnIndexManagerProvider.getIndexManager(table, column);
            }
        }

        schema.addTable(table);

        persistSchema();
    }

    @Override
    public synchronized <K extends Number & Comparable<K>> int deleteTable(String tableName) throws SchemaException, IOException, ExecutionException, InterruptedException, StorageException, DbException {
        validateSchemaExists();

        Optional<Table> optionalTable = SchemaSearcher.findTable(schema, tableName);
        if (optionalTable.isEmpty()) {
            throw new SchemaException(new TableNotPresentError(tableName));
        }

        schema.removeTable(tableName);

        persistSchema();

        Table table = optionalTable.get();

        int rowCount = 0;

        IndexManager<K, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        LockableIterator<LeafTreeNode.KeyValue<K, Pointer>> sortedIterator = clusterIndexManager.getSortedIterator();
        try {
            sortedIterator.lock();
            while (sortedIterator.hasNext()) {
                LeafTreeNode.KeyValue<K, Pointer> keyValue = sortedIterator.next();
                databaseStorageManager.remove(keyValue.value());
                rowCount++;
            }
        } finally {
            sortedIterator.unlock();
            columnIndexManagerProvider.clearIndexManager(table, SchemaSearcher.findClusterColumn(table));
        }

        for (Column column : table.getColumns()) {
            if (column.isUnique()) {
                IndexManager<?, ?> indexManager = columnIndexManagerProvider.getIndexManager(table, column);
                indexManager.purgeIndex();

                columnIndexManagerProvider.clearIndexManager(table, column);
            }
        }

        return rowCount;
    }

    @Override
    public synchronized <K extends Number & Comparable<K>> int createIndex(String tableName, Index index) throws SchemaException, StorageException, DbException, DeserializationException, BTreeException, SerializationException {
        validateSchemaExists();

        Optional<Table> optionalTable = SchemaSearcher.findTable(schema, tableName);
        if (optionalTable.isEmpty()) {
            throw new SchemaException(new TableNotPresentError(tableName));
        }

        Table table = optionalTable.get();
        table.addIndex(index);

        Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, index.getColumnName());
        if (optionalColumn.isEmpty()) {
            throw new SchemaException(new ColumnNotPresentError(index.getColumnName(), tableName));
        }

        Column column = optionalColumn.get();
        column.addConstraint(SqlConstraint.Unique);

        persistSchema();

        int rowCount = 0;

        IndexManager<K, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        IndexManager<?, K> indexManager = columnIndexManagerProvider.getIndexManager(table, optionalColumn.get());
        LockableIterator<LeafTreeNode.KeyValue<K, Pointer>> sortedIterator = clusterIndexManager.getSortedIterator();
        try {
            sortedIterator.lock();
            while (sortedIterator.hasNext()) {
                LeafTreeNode.KeyValue<K, Pointer> keyValue = sortedIterator.next();

                Optional<DbObject> optionalDbObject = databaseStorageManager.select(keyValue.value());
                if (optionalDbObject.isEmpty()) {
                    continue;
                }

                DbObject dbObject = optionalDbObject.get();

                indexManager.addIndex(SerializationUtils.getValueOfFieldAsObject(table, column, dbObject.getData()), keyValue.key());
                rowCount++;
            }
        } finally {
            sortedIterator.unlock();
        }

        return rowCount;
    }

    private void validateSchemaExists() throws SchemaException {
        if (Objects.isNull(schema)) {
            throw new SchemaException(new SchemaDoesNotExistError());
        }
    }
}
