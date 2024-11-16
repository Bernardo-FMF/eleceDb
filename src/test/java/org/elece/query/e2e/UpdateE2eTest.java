package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.DbObject;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.statement.UpdateStatement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.elece.utils.SerializationUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UpdateE2eTest {
    private static DbConfig dbConfig;
    private static DependencyContainer dependencyContainer;

    @BeforeAll
    static void setUp() throws IOException, SchemaException, ParserException, AnalyzerException, TokenizerException,
                               BTreeException, QueryException, SerializationException, InterruptedTaskException,
                               StorageException, DeserializationException, ProtoException, FileChannelException,
                               DbException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setPort(3000)
                .setBaseDbPath(Files.createTempDirectory("Update_E2e_Test").toString())
                .setSessionStrategy(DbConfig.SessionStrategy.IMMEDIATE)
                .build();

        dependencyContainer = new DependencyContainer(dbConfig);

        dependencyContainer.getSchemaManager().createSchema("usersDb");

        E2eUtils.createTable(dependencyContainer);
        E2eUtils.insertRows(dependencyContainer);
    }

    @AfterAll
    static void tearDown() throws IOException {
        FileTestUtils.deleteDirectory(dbConfig.getBaseDbPath());
    }

    @Test
    @Order(1)
    void test_updateWithPrimaryKeyWhere() throws SchemaException, ParserException, BTreeException, QueryException,
                                                 SerializationException, InterruptedTaskException, StorageException,
                                                 DeserializationException, ProtoException, FileChannelException,
                                                 DbException, AnalyzerException, TokenizerException {
        planAndValidateQuery(
                "UPDATE users SET name = \"newName1\" WHERE id = 1;",
                List.of(1),
                Map.of(
                        SchemaSearcher.findColumn(dependencyContainer.getSchemaManager().getSchema().getTables().getFirst(), "name").get(), "newName1"
                ));
    }

    private static void planAndValidateQuery(String query, List<Integer> affectedRows,
                                             Map<Column, Object> newValueMap) throws
                                                                              SchemaException,
                                                                              ParserException,
                                                                              BTreeException,
                                                                              QueryException,
                                                                              SerializationException,
                                                                              InterruptedTaskException,
                                                                              StorageException,
                                                                              DeserializationException,
                                                                              ProtoException,
                                                                              FileChannelException,
                                                                              DbException,
                                                                              AnalyzerException,
                                                                              TokenizerException {
        UpdateStatement updateStatement = (UpdateStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(), query);
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(updateStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response updateResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.UPDATE, updateResponse.responseType());
        Assertions.assertEquals(String.valueOf(affectedRows.size()), E2eUtils.extractValue("RowCount", updateResponse.response()));

        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        IndexManager<Integer, Pointer> clusterIndexManager = dependencyContainer.getColumnIndexManagerProvider().getClusterIndexManager(table);
        for (Integer affectedRow : affectedRows) {
            Optional<Pointer> affectedPointer = clusterIndexManager.getIndex(affectedRow);
            Assertions.assertTrue(affectedPointer.isPresent());
            Optional<DbObject> optionalDbObject = dependencyContainer.getDatabaseStorageManager().select(affectedPointer.get());
            Assertions.assertTrue(optionalDbObject.isPresent());
            DbObject dbObject = optionalDbObject.get();

            for (Map.Entry<Column, Object> entry : newValueMap.entrySet()) {
                Column column = entry.getKey();
                byte[] newValue = SerializationUtils.getValueOfField(table, column, dbObject);
                SerializerRegistry serializerRegistry = dependencyContainer.getSerializerRegistry();
                Serializer<?> serializer = serializerRegistry.getSerializer(column.getSqlType().getType());
                Object deserializedNewValue = serializer.deserialize(newValue, column);
                if (deserializedNewValue instanceof String stringValue) {
                    Assertions.assertEquals(entry.getValue(), stringValue.trim());
                } else {
                    Assertions.assertEquals(entry.getValue(), deserializedNewValue);
                }
            }
        }
    }

    @Test
    @Order(2)
    void test_updateWithNonIndexedWhere() throws SchemaException, ParserException, BTreeException, QueryException,
                                                 SerializationException, InterruptedTaskException, StorageException,
                                                 DeserializationException, ProtoException, FileChannelException,
                                                 DbException, AnalyzerException, TokenizerException {
        planAndValidateQuery(
                "UPDATE users SET name = \"newName2\" WHERE isAdmin = true;",
                List.of(1),
                Map.of(
                        SchemaSearcher.findColumn(dependencyContainer.getSchemaManager().getSchema().getTables().getFirst(), "name").get(), "newName2"
                ));
    }

    @Test
    @Order(3)
    void test_updateWithComplexWhere() throws SchemaException, ParserException, BTreeException, QueryException,
                                              SerializationException, InterruptedTaskException, StorageException,
                                              DeserializationException, ProtoException, FileChannelException,
                                              DbException, AnalyzerException, TokenizerException {
        planAndValidateQuery(
                "UPDATE users SET name = \"newName3\" WHERE (id = 3 OR id = 4) AND isAdmin = false;",
                List.of(3, 4),
                Map.of(
                        SchemaSearcher.findColumn(dependencyContainer.getSchemaManager().getSchema().getTables().getFirst(), "name").get(), "newName3"
                ));
    }

    @Test
    @Order(4)
    void test_updateWithoutWhere() throws SchemaException, ParserException, BTreeException, QueryException,
                                          SerializationException, InterruptedTaskException, StorageException,
                                          DeserializationException, ProtoException, FileChannelException,
                                          DbException, AnalyzerException, TokenizerException {
        planAndValidateQuery(
                "UPDATE users SET name = \"newName4\";",
                List.of(1, 2, 3, 4, 5),
                Map.of(
                        SchemaSearcher.findColumn(dependencyContainer.getSchemaManager().getSchema().getTables().getFirst(), "name").get(), "newName4"
                ));
    }

    @Test
    @Order(5)
    void test_updatePrimaryKey() throws SchemaException, ParserException, BTreeException, QueryException,
                                        SerializationException, InterruptedTaskException, StorageException,
                                        DeserializationException, ProtoException, FileChannelException,
                                        DbException, AnalyzerException, TokenizerException {
        planAndValidateQuery(
                "UPDATE users SET id = 6, isAdmin = false WHERE id = 1;",
                List.of(1),
                Map.of(
                        SchemaSearcher.findColumn(dependencyContainer.getSchemaManager().getSchema().getTables().getFirst(), "id").get(), 6,
                        SchemaSearcher.findColumn(dependencyContainer.getSchemaManager().getSchema().getTables().getFirst(), "isAdmin").get(), false
                ));
    }
}
