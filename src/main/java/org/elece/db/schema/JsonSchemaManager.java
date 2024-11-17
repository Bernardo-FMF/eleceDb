package org.elece.db.schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elece.config.DbConfig;
import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.db.schema.model.builder.SchemaBuilder;
import org.elece.exception.*;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class JsonSchemaManager implements SchemaManager {
    private final Logger logger = LogManager.getLogger(JsonSchemaManager.class);

    private Schema schema;
    private final DbConfig dbConfig;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final DatabaseStorageManager databaseStorageManager;
    private final Gson gson;
    private final AtomicInteger tableIndex;

    public JsonSchemaManager(DbConfig dbConfig, ColumnIndexManagerProvider columnIndexManagerProvider,
                             DatabaseStorageManager databaseStorageManager) throws SchemaException {
        this.dbConfig = dbConfig;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.databaseStorageManager = databaseStorageManager;
        this.gson = new GsonBuilder().serializeNulls().create();
        loadSchema();

        tableIndex = new AtomicInteger(Objects.isNull(schema) || schema.getTables().isEmpty() ? 0 : schema.getTables().getLast().getId());
    }

    private void loadSchema() throws SchemaException {
        logger.info("Loading schema into memory");
        String schemePath = getSchemePath();
        if (!Files.exists(Path.of(schemePath))) {
            logger.info("Schema not found in '{}'", schemePath);
            this.schema = null;
            return;
        }
        try {
            FileReader fileReader = new FileReader(schemePath);
            JsonReader jsonReader = new JsonReader(fileReader);
            this.schema = gson.fromJson(jsonReader, Schema.class);
            logger.info("Loaded schema: {}", this.schema);
            fileReader.close();
        } catch (IOException exception) {
            throw new SchemaException(DbError.SCHEMA_PERSISTENCE_ERROR, exception.getMessage());
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
            logger.info("Updated schema in disk: {}", this.schema);
        } catch (IOException exception) {
            throw new SchemaException(DbError.SCHEMA_PERSISTENCE_ERROR, exception.getMessage());
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public synchronized void createSchema(String dbName) throws SchemaException {
        if (!Objects.isNull(schema)) {
            throw new SchemaException(DbError.SCHEMA_ALREADY_EXISTS_ERROR, "Database schema is already defined");
        }

        this.schema = SchemaBuilder.builder()
                .setDbName(dbName)
                .setTables(new ArrayList<>())
                .build();
        persistSchema();
        logger.info("Created new schema: {}", this.schema);
    }

    @Override
    public int deleteSchema() throws SchemaException, StorageException, DbException, InterruptedTaskException,
                                     FileChannelException {
        validateSchemaExists();

        int totalRowCount = 0;
        for (Table table : schema.getTables()) {
            totalRowCount += deleteTable(table.getName());
        }

        this.schema = null;

        persistSchema();

        logger.info("Deleted existing schema: {}; Removed {} rows", this.schema, totalRowCount);
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
        logger.info("Created new table {}", table);
    }

    @Override
    public synchronized <K extends Number & Comparable<K>> int deleteTable(String tableName) throws SchemaException,
                                                                                                    StorageException,
                                                                                                    DbException,
                                                                                                    InterruptedTaskException,
                                                                                                    FileChannelException {
        validateSchemaExists();

        Optional<Table> optionalTable = SchemaSearcher.findTable(schema, tableName);
        if (optionalTable.isEmpty()) {
            throw new SchemaException(DbError.TABLE_NOT_FOUND_ERROR, String.format("Table %s is not present in the database schema", tableName));
        }

        schema.removeTable(tableName);

        persistSchema();

        Table table = optionalTable.get();

        int rowCount = 0;

        IndexManager<K, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        LockableIterator<LeafTreeNode.KeyValue<K, Pointer>> sortedIterator = clusterIndexManager.getSortedIterator();
        logger.info("Deleted table {}; Trying to remove all indexes", tableName);
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
        logger.info("Cleared cluster index and all data from disk from table {}", tableName);

        logger.info("Removing all related indexes from table {}", tableName);
        for (Column column : table.getColumns()) {
            if (column.isUnique()) {
                logger.info("Clearing indexed column {}", column.getName());
                IndexManager<?, ?> indexManager = columnIndexManagerProvider.getIndexManager(table, column);
                indexManager.purgeIndex();

                columnIndexManagerProvider.clearIndexManager(table, column);
            }
        }

        logger.info("Deleted table {}; Removed {} rows", tableName, rowCount);
        return rowCount;
    }

    @Override
    public synchronized <K extends Number & Comparable<K>> int createIndex(String tableName, Index index) throws
                                                                                                          SchemaException,
                                                                                                          StorageException,
                                                                                                          DbException,
                                                                                                          DeserializationException,
                                                                                                          BTreeException,
                                                                                                          SerializationException,
                                                                                                          InterruptedTaskException,
                                                                                                          FileChannelException {
        validateSchemaExists();

        Optional<Table> optionalTable = SchemaSearcher.findTable(schema, tableName);
        if (optionalTable.isEmpty()) {
            throw new SchemaException(DbError.TABLE_NOT_FOUND_ERROR, String.format("Table %s is not present in the database schema", tableName));
        }

        Table table = optionalTable.get();
        table.addIndex(index);

        Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, index.getColumnName());
        if (optionalColumn.isEmpty()) {
            throw new SchemaException(DbError.COLUMN_NOT_FOUND_ERROR, String.format("Column %s is not present in the table %s", index.getColumnName(), tableName));
        }

        Column column = optionalColumn.get();
        column.addConstraint(SqlConstraint.Unique);

        persistSchema();

        int rowCount = 0;

        IndexManager<K, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        IndexManager<?, K> indexManager = columnIndexManagerProvider.getIndexManager(table, optionalColumn.get());
        LockableIterator<LeafTreeNode.KeyValue<K, Pointer>> sortedIterator = clusterIndexManager.getSortedIterator();

        logger.info("Creating new index {} on table {}; Trying to update indexes", index, tableName);
        try {
            sortedIterator.lock();
            while (sortedIterator.hasNext()) {
                LeafTreeNode.KeyValue<K, Pointer> keyValue = sortedIterator.next();

                Optional<DbObject> optionalDbObject = databaseStorageManager.select(keyValue.value());
                if (optionalDbObject.isEmpty() || !optionalDbObject.get().isAlive()) {
                    continue;
                }

                DbObject dbObject = optionalDbObject.get();

                indexManager.addIndex(SerializationUtils.getValueOfFieldAsObject(table, column, dbObject.getData()), keyValue.key());
                rowCount++;
            }
        } finally {
            sortedIterator.unlock();
        }

        logger.info("Created new index {} on table {}; Affected {} rows", index, tableName, rowCount);
        return rowCount;
    }

    private void validateSchemaExists() throws SchemaException {
        if (Objects.isNull(schema)) {
            throw new SchemaException(DbError.SCHEMA_NOT_FOUND_ERROR, "Database schema is not defined");
        }
    }
}
