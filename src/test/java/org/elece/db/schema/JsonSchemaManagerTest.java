package org.elece.db.schema;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.DiskPageDatabaseStorageManager;
import org.elece.db.InMemoryReservedSlotTracer;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.DefaultColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.storage.file.DefaultFileHandlerFactory;
import org.elece.storage.file.DefaultFileHandlerPoolFactory;
import org.elece.storage.file.UnrestrictedFileHandlerPool;
import org.elece.storage.index.DefaultIndexStorageManagerFactory;
import org.elece.storage.index.header.DefaultIndexHeaderManagerFactory;
import org.elece.utils.BinaryUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

class JsonSchemaManagerTest {
    private static final int VARCHAR_MAX_SIZE = 255;

    private static final String TABLE_NAME = "user_table";
    private static final String COLUMN_ID_PRIMARY = "id";
    private static final String COLUMN_NAME_NORMAL = "name";

    private DbConfig dbConfig;
    private Table table;

    @BeforeEach
    void setup() throws IOException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setBaseDbPath(Files.createTempDirectory("Json_Schema_Manager_Test").toString())
                .build();

        Column clusterColumn = new Column(CLUSTER_ID, SqlType.intType, new ArrayList<>(List.of(SqlConstraint.UNIQUE)));
        clusterColumn.setId(1);
        Column idColumn = new Column(COLUMN_ID_PRIMARY, SqlType.intType, new ArrayList<>(List.of(SqlConstraint.PRIMARY_KEY)));
        idColumn.setId(2);
        Column nameColumn = new Column(COLUMN_NAME_NORMAL, SqlType.varcharType, new ArrayList<>());
        nameColumn.setId(3);
        table = new Table(TABLE_NAME, Arrays.asList(clusterColumn, idColumn, nameColumn), new ArrayList<>());
    }

    @Test
    void test_createSchema() throws SchemaException {
        DatabaseStorageManager databaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), dbConfig), new InMemoryReservedSlotTracer());
        ColumnIndexManagerProvider columnIndexManagerProvider = new DefaultColumnIndexManagerProvider(dbConfig, new DefaultIndexStorageManagerFactory(dbConfig, new DefaultFileHandlerPoolFactory(dbConfig), new DefaultIndexHeaderManagerFactory()));

        SchemaManager schemaManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        schemaManager.createSchema("new_db");

        SchemaManager validationManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        assertSchema(schemaManager.getSchema(), validationManager.getSchema());
    }

    @Test
    void test_createTable() throws SchemaException, StorageException {
        DatabaseStorageManager databaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), dbConfig), new InMemoryReservedSlotTracer());
        ColumnIndexManagerProvider columnIndexManagerProvider = new DefaultColumnIndexManagerProvider(dbConfig, new DefaultIndexStorageManagerFactory(dbConfig, new DefaultFileHandlerPoolFactory(dbConfig), new DefaultIndexHeaderManagerFactory()));

        SchemaManager schemaManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        schemaManager.createSchema("new_db");
        schemaManager.createTable(table);

        Optional<Table> userTable = SchemaSearcher.findTable(schemaManager.getSchema(), TABLE_NAME);
        Assertions.assertTrue(userTable.isPresent());
        Assertions.assertEquals(2, userTable.get().getIndexes().size());

        Assertions.assertEquals("cluster_index", userTable.get().getIndexes().get(0).getName());
        Assertions.assertEquals(CLUSTER_ID, userTable.get().getIndexes().get(0).getColumnName());

        Assertions.assertEquals("col_index_2", userTable.get().getIndexes().get(1).getName());
        Assertions.assertEquals("id", userTable.get().getIndexes().get(1).getColumnName());

        SchemaManager validationManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        assertSchema(schemaManager.getSchema(), validationManager.getSchema());
    }

    @Test
    void test_createIndex() throws SchemaException, StorageException, BTreeException, SerializationException,
                                          DeserializationException, DbException, InterruptedTaskException,
                                          FileChannelException {
        DatabaseStorageManager databaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), dbConfig), new InMemoryReservedSlotTracer());
        ColumnIndexManagerProvider columnIndexManagerProvider = new DefaultColumnIndexManagerProvider(dbConfig, new DefaultIndexStorageManagerFactory(dbConfig, new DefaultFileHandlerPoolFactory(dbConfig), new DefaultIndexHeaderManagerFactory()));

        Index index = new Index("name_index", COLUMN_NAME_NORMAL);

        SchemaManager schemaManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        schemaManager.createSchema("new_db");
        schemaManager.createTable(table);

        int rowCount = schemaManager.createIndex(TABLE_NAME, index);
        Assertions.assertEquals(0, rowCount);

        Optional<Table> fetchedTable = SchemaSearcher.findTable(schemaManager.getSchema(), TABLE_NAME);
        Assertions.assertTrue(fetchedTable.isPresent());
        Assertions.assertTrue(fetchedTable.get().getIndexes().contains(index));

        SchemaManager validationManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        assertSchema(schemaManager.getSchema(), validationManager.getSchema());
    }

    @Test
    void test_createIndexWithPreviousData() throws SchemaException, StorageException, DbException,
                                                          BTreeException, SerializationException,
                                                          DeserializationException, InterruptedTaskException,
                                                          FileChannelException {
        // create schema and table
        DatabaseStorageManager databaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), dbConfig), new InMemoryReservedSlotTracer());
        ColumnIndexManagerProvider columnIndexManagerProvider = new DefaultColumnIndexManagerProvider(dbConfig, new DefaultIndexStorageManagerFactory(dbConfig, new DefaultFileHandlerPoolFactory(dbConfig), new DefaultIndexHeaderManagerFactory()));

        SchemaManager schemaManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        schemaManager.createSchema("new_db");
        schemaManager.createTable(table);

        byte[] row1 = new byte[Integer.BYTES + Integer.BYTES + VARCHAR_MAX_SIZE];
        System.arraycopy(BinaryUtils.integerToBytes(1), 0, row1, 0, Integer.BYTES);
        System.arraycopy(BinaryUtils.integerToBytes(1), 0, row1, Integer.BYTES, Integer.BYTES);
        System.arraycopy(BinaryUtils.stringToBytes("user1"), 0, row1, Integer.BYTES * 2, 5);
        BinaryUtils.fillPadding(Integer.BYTES + Integer.BYTES + 5, row1.length, row1);

        Pointer row1Pointer = databaseStorageManager.store(table.getId(), row1);
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        IndexManager<Integer, Integer> idIndexManager = columnIndexManagerProvider.getIndexManager(table, SchemaSearcher.findColumn(table, COLUMN_ID_PRIMARY).get());

        clusterIndexManager.addIndex(1, row1Pointer);
        idIndexManager.addIndex(1, 1);

        Optional<Pointer> clusterIndex = clusterIndexManager.getIndex(1);
        Optional<Integer> idIndex = idIndexManager.getIndex(1);

        Assertions.assertTrue(clusterIndex.isPresent());
        Assertions.assertTrue(idIndex.isPresent());
        Assertions.assertEquals(row1Pointer, clusterIndex.get());
        Assertions.assertEquals(1, idIndex.get());

        Optional<DbObject> optionalDbObject = databaseStorageManager.select(clusterIndex.get());
        Assertions.assertTrue(optionalDbObject.isPresent());

        Index newIndex = new Index("name_index", COLUMN_NAME_NORMAL);
        int rowCount = schemaManager.createIndex(TABLE_NAME, newIndex);
        Assertions.assertEquals(1, rowCount);

        SchemaManager validationManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        assertSchema(schemaManager.getSchema(), validationManager.getSchema());
    }

    @Test
    void test_deleteTable() throws SchemaException, StorageException, DbException, InterruptedTaskException,
                                          FileChannelException {
        DatabaseStorageManager databaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), dbConfig), new InMemoryReservedSlotTracer());
        ColumnIndexManagerProvider columnIndexManagerProvider = new DefaultColumnIndexManagerProvider(dbConfig, new DefaultIndexStorageManagerFactory(dbConfig, new DefaultFileHandlerPoolFactory(dbConfig), new DefaultIndexHeaderManagerFactory()));

        SchemaManager schemaManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        schemaManager.createSchema("new_db");
        schemaManager.createTable(table);

        Assertions.assertTrue(SchemaSearcher.findTable(schemaManager.getSchema(), TABLE_NAME).isPresent());

        int rowCount = schemaManager.deleteTable(TABLE_NAME);
        Assertions.assertEquals(0, rowCount);

        SchemaManager validationManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        assertSchema(schemaManager.getSchema(), validationManager.getSchema());
    }

    @Test
    void test_deleteTableWithPreviousData() throws SchemaException, StorageException, DbException,
                                                          BTreeException, SerializationException,
                                                          InterruptedTaskException, FileChannelException {
        DatabaseStorageManager databaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), dbConfig), new InMemoryReservedSlotTracer());
        ColumnIndexManagerProvider columnIndexManagerProvider = new DefaultColumnIndexManagerProvider(dbConfig, new DefaultIndexStorageManagerFactory(dbConfig, new DefaultFileHandlerPoolFactory(dbConfig), new DefaultIndexHeaderManagerFactory()));

        SchemaManager schemaManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        schemaManager.createSchema("new_db");
        schemaManager.createTable(table);

        byte[] row1 = new byte[Integer.BYTES + Integer.BYTES + VARCHAR_MAX_SIZE];
        System.arraycopy(BinaryUtils.integerToBytes(1), 0, row1, 0, Integer.BYTES);
        System.arraycopy(BinaryUtils.integerToBytes(1), 0, row1, Integer.BYTES, Integer.BYTES);
        System.arraycopy(BinaryUtils.stringToBytes("user1"), 0, row1, Integer.BYTES * 2, 5);
        BinaryUtils.fillPadding(Integer.BYTES + Integer.BYTES + 5, row1.length, row1);

        Pointer row1Pointer = databaseStorageManager.store(table.getId(), row1);
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        IndexManager<Integer, Integer> idIndexManager = columnIndexManagerProvider.getIndexManager(table, SchemaSearcher.findColumn(table, COLUMN_ID_PRIMARY).get());

        clusterIndexManager.addIndex(1, row1Pointer);
        idIndexManager.addIndex(1, 1);

        Assertions.assertTrue(SchemaSearcher.findTable(schemaManager.getSchema(), TABLE_NAME).isPresent());

        int rowCount = schemaManager.deleteTable(TABLE_NAME);
        Assertions.assertEquals(1, rowCount);

        Optional<DbObject> optionalDbObject = databaseStorageManager.select(row1Pointer);
        Assertions.assertTrue(optionalDbObject.isPresent());
        Assertions.assertFalse(optionalDbObject.get().isAlive());

        SchemaManager validationManager = new JsonSchemaManager(dbConfig, columnIndexManagerProvider, databaseStorageManager);
        assertSchema(schemaManager.getSchema(), validationManager.getSchema());
    }

    private void assertSchema(Schema oldSchema, Schema newSchema) {
        Assertions.assertNotNull(oldSchema);
        Assertions.assertNotNull(newSchema);

        Assertions.assertEquals(oldSchema, newSchema);
    }
}