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

/**
 * The {@link JsonSchemaManager} class is responsible for managing the JSON-based schema of the database.
 * It handles schema creation, deletion, table management, and indexing.
 * This manager ensures that schema modifications are persisted to disk and provides functionality to load the schema into memory.
 */
public class JsonSchemaManager implements SchemaManager {
    private final Logger logger = LogManager.getLogger(JsonSchemaManager.class);

    private Schema schema;
    private final DbConfig dbConfig;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final DatabaseStorageManager databaseStorageManager;
    private final Gson gson;
    private final AtomicInteger tableIndex;

    /**
     * Constructs a new JsonSchemaManager. Initializes the schema and table index.
     *
     * @param dbConfig                   The database configuration settings to be used by the schema manager.
     * @param columnIndexManagerProvider The provider for managing column indexes.
     * @param databaseStorageManager     The storage manager responsible for handling database storage operations.
     * @throws SchemaException If any error occurs while loading the schema into memory.
     */
    public JsonSchemaManager(DbConfig dbConfig, ColumnIndexManagerProvider columnIndexManagerProvider,
                             DatabaseStorageManager databaseStorageManager) throws SchemaException {
        this.dbConfig = dbConfig;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.databaseStorageManager = databaseStorageManager;
        this.gson = new GsonBuilder().serializeNulls().create();
        loadSchema();

        tableIndex = new AtomicInteger(Objects.isNull(schema) || schema.getTables().isEmpty() ? 0 : schema.getTables().getLast().getId());
    }

    /**
     * Loads the schema from a file into memory.
     * If the schema file is not found, we can assume there is no schema yet.
     * If the file exists, it reads the schema from the file, and deserializes the JSON into a {@link Schema}.
     * Any I/O errors encountered during this process will result in a SchemaException being thrown.
     *
     * @throws SchemaException If any error occurs while reading the schema file or deserializing the schema.
     */
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

    /**
     * Persists the current schema to disk by writing it to the JSON file.
     *
     * @throws SchemaException If an I/O error occurs while attempting to write the schema to disk.
     */
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


    /**
     * Retrieves the current schema managed by the JsonSchemaManager.
     *
     * @return The current {@link Schema} instance.
     */
    @Override
    public Schema getSchema() {
        return schema;
    }

    /**
     * Creates a new schema for the specified database name.
     *
     * @param dbName The name of the database for which the schema is being created.
     * @throws SchemaException If the schema is already defined or if an error occurs during schema creation.
     */
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

    /**
     * Deletes the existing schema along with all the tables and their data.
     *
     * @return The total number of rows removed from all the tables in the schema.
     * @throws SchemaException          If there is an issue with the schema.
     * @throws StorageException         If there is an issue with storage operations.
     * @throws DbException              If there is a generic database-related exception.
     * @throws InterruptedTaskException If the task is interrupted during execution.
     * @throws FileChannelException     If there is an issue with file channel operations.
     */
    @Override
    public synchronized int deleteSchema() throws SchemaException, StorageException, DbException,
                                                  InterruptedTaskException,
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

    /**
     * Creates a new table within the current schema and sets up necessary indexes based on the columns in the table.
     *
     * @param table The table to be created, containing column definitions and their constraints.
     * @throws SchemaException  If there is an issue with the schema, such as the schema not existing.
     * @throws StorageException If there is an issue with storage operations.
     */
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

    /**
     * Deletes a table from the schema and removes all associated data and indexes.
     *
     * @param tableName The name of the table to be deleted.
     * @return The number of rows removed from the deleted table.
     * @throws SchemaException          If the schema does not exist or the table is not found in the schema.
     * @throws StorageException         If there is an issue with storage operations while deleting the table.
     * @throws DbException              If any database-related error occurs during the operation.
     * @throws InterruptedTaskException If the task is interrupted during execution.
     * @throws FileChannelException     If there is an issue with file channel operations while deleting the table.
     */
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
            clusterIndexManager.purgeIndex();
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

    /**
     * Creates an index on a specified table. The index ensures that the column specified is unique and updates the schema
     * accordingly. The process of creating an index involves reading all rows from disk, extracting the value of the column being indexed, and add those files to the new index.
     *
     * @param <K>       The type of keys used in the index, which must be compatible with the clustered column.
     * @param tableName The name of the table on which the index is to be created.
     * @param index     The index definition, including the column to be indexed.
     * @return The number of rows that were affected by creating the new index.
     * @throws SchemaException          If there are errors related to the schema, such as the table or column not being found.
     * @throws StorageException         If there are issues with storage operations.
     * @throws DbException              If there is a generic database-related error.
     * @throws DeserializationException If there are errors deserializing data during the operation.
     * @throws BTreeException           If there are errors related to B-tree structure while managing the index.
     * @throws SerializationException   If there are errors serializing data during the operation.
     * @throws InterruptedTaskException If the task is interrupted during execution.
     * @throws FileChannelException     If there are issues with file channel operations.
     */
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
        column.addConstraint(SqlConstraint.UNIQUE);

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
