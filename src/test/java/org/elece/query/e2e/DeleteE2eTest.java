package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.sql.parser.statement.CreateTableStatement;
import org.elece.sql.parser.statement.DeleteStatement;
import org.elece.sql.parser.statement.InsertStatement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DeleteE2eTest {
    private static DbConfig dbConfig;
    private static DependencyContainer dependencyContainer;

    @BeforeAll
    static void setUp() throws IOException, SchemaException, ParserException, AnalyzerException, TokenizerException,
                               BTreeException, QueryException, SerializationException, InterruptedTaskException,
                               StorageException, DeserializationException, ProtoException, FileChannelException,
                               DbException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setPort(3000)
                .setBaseDbPath(Files.createTempDirectory("Delete_E2e_Test").toString())
                .setSessionStrategy(DbConfig.SessionStrategy.IMMEDIATE)
                .build();

        dependencyContainer = new DependencyContainer(dbConfig);

        dependencyContainer.getSchemaManager().createSchema("usersDb");

        CreateTableStatement createTableStatement = (CreateTableStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), username VARCHAR(255) UNIQUE, isAdmin BOOL);");
        dependencyContainer.getQueryPlanner().plan(createTableStatement, new MockedClientInterface());

        MockedClientInterface clientInterface = new MockedClientInterface();
        InsertStatement insertStatement1 = (InsertStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "INSERT INTO users (id, name, username, isAdmin) VALUES (1, \"name1\", \"username1\", true);");
        InsertStatement insertStatement2 = (InsertStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "INSERT INTO users (id, name, username, isAdmin) VALUES (2, \"name2\", \"username2\", false);");
        InsertStatement insertStatement3 = (InsertStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "INSERT INTO users (id, name, username, isAdmin) VALUES (3, \"name3\", \"username3\", false);");
        InsertStatement insertStatement4 = (InsertStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "INSERT INTO users (id, name, username, isAdmin) VALUES (4, \"name4\", \"username4\", false);");
        InsertStatement insertStatement5 = (InsertStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "INSERT INTO users (id, name, username, isAdmin) VALUES (5, \"name5\", \"username5\", false);");

        dependencyContainer.getQueryPlanner().plan(insertStatement1, clientInterface);
        dependencyContainer.getQueryPlanner().plan(insertStatement2, clientInterface);
        dependencyContainer.getQueryPlanner().plan(insertStatement3, clientInterface);
        dependencyContainer.getQueryPlanner().plan(insertStatement4, clientInterface);
        dependencyContainer.getQueryPlanner().plan(insertStatement5, clientInterface);
    }

    @AfterAll
    static void tearDown() throws IOException {
        FileTestUtils.deleteDirectory(dbConfig.getBaseDbPath());
    }

    @Test
    @Order(1)
    void test_deleteWithPrimaryKeyWhere() throws SchemaException, ParserException, AnalyzerException,
                                                 TokenizerException,
                                                 BTreeException, QueryException, SerializationException,
                                                 InterruptedTaskException,
                                                 StorageException, DeserializationException, ProtoException,
                                                 FileChannelException,
                                                 DbException {
        DeleteStatement deleteStatement = (DeleteStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "DELETE FROM users WHERE id = 2;");
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(deleteStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response deleteResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.DELETE, deleteResponse.responseType());
        Assertions.assertEquals("1", E2eUtils.extractValue("RowCount", deleteResponse.response()));

        ColumnIndexManagerProvider columnIndexManagerProvider = dependencyContainer.getColumnIndexManagerProvider();

        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

        Optional<Pointer> indexPointer1 = clusterIndexManager.getIndex(1);
        Assertions.assertTrue(indexPointer1.isPresent());
        Optional<Pointer> indexPointer2 = clusterIndexManager.getIndex(2);
        Assertions.assertTrue(indexPointer2.isEmpty());
        Optional<Pointer> indexPointer3 = clusterIndexManager.getIndex(3);
        Assertions.assertTrue(indexPointer3.isPresent());
        Optional<Pointer> indexPointer4 = clusterIndexManager.getIndex(4);
        Assertions.assertTrue(indexPointer4.isPresent());
        Optional<Pointer> indexPointer5 = clusterIndexManager.getIndex(5);
        Assertions.assertTrue(indexPointer5.isPresent());
    }

    @Test
    @Order(2)
    void test_deleteWithNonIndexedWhere() throws SchemaException, ParserException, AnalyzerException,
                                                 TokenizerException,
                                                 BTreeException, QueryException, SerializationException,
                                                 InterruptedTaskException,
                                                 StorageException, DeserializationException, ProtoException,
                                                 FileChannelException,
                                                 DbException {
        DeleteStatement deleteStatement = (DeleteStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "DELETE FROM users WHERE isAdmin = true;");
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(deleteStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response deleteResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.DELETE, deleteResponse.responseType());
        Assertions.assertEquals("1", E2eUtils.extractValue("RowCount", deleteResponse.response()));

        ColumnIndexManagerProvider columnIndexManagerProvider = dependencyContainer.getColumnIndexManagerProvider();

        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

        Optional<Pointer> indexPointer1 = clusterIndexManager.getIndex(1);
        Assertions.assertTrue(indexPointer1.isEmpty());
        Optional<Pointer> indexPointer2 = clusterIndexManager.getIndex(2);
        Assertions.assertTrue(indexPointer2.isEmpty());
        Optional<Pointer> indexPointer3 = clusterIndexManager.getIndex(3);
        Assertions.assertTrue(indexPointer3.isPresent());
        Optional<Pointer> indexPointer4 = clusterIndexManager.getIndex(4);
        Assertions.assertTrue(indexPointer4.isPresent());
        Optional<Pointer> indexPointer5 = clusterIndexManager.getIndex(5);
        Assertions.assertTrue(indexPointer5.isPresent());
    }

    @Test
    @Order(3)
    void test_deleteWithComplexWhere() throws SchemaException, ParserException, AnalyzerException, TokenizerException,
                                              BTreeException, QueryException, SerializationException,
                                              InterruptedTaskException,
                                              StorageException, DeserializationException, ProtoException,
                                              FileChannelException,
                                              DbException {
        DeleteStatement deleteStatement = (DeleteStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "DELETE FROM users WHERE (id = 3 OR id = 4) AND isAdmin = false;");
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(deleteStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response deleteResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.DELETE, deleteResponse.responseType());
        Assertions.assertEquals("2", E2eUtils.extractValue("RowCount", deleteResponse.response()));

        ColumnIndexManagerProvider columnIndexManagerProvider = dependencyContainer.getColumnIndexManagerProvider();

        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

        Optional<Pointer> indexPointer1 = clusterIndexManager.getIndex(1);
        Assertions.assertTrue(indexPointer1.isEmpty());
        Optional<Pointer> indexPointer2 = clusterIndexManager.getIndex(2);
        Assertions.assertTrue(indexPointer2.isEmpty());
        Optional<Pointer> indexPointer3 = clusterIndexManager.getIndex(3);
        Assertions.assertTrue(indexPointer3.isEmpty());
        Optional<Pointer> indexPointer4 = clusterIndexManager.getIndex(4);
        Assertions.assertTrue(indexPointer4.isEmpty());
        Optional<Pointer> indexPointer5 = clusterIndexManager.getIndex(5);
        Assertions.assertTrue(indexPointer5.isPresent());
    }

    @Test
    @Order(4)
    void test_deleteWithoutWhere() throws SchemaException, ParserException, AnalyzerException, TokenizerException,
                                          BTreeException, QueryException, SerializationException,
                                          InterruptedTaskException,
                                          StorageException, DeserializationException, ProtoException,
                                          FileChannelException,
                                          DbException {
        DeleteStatement deleteStatement = (DeleteStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "DELETE FROM users;");
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(deleteStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response deleteResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.DELETE, deleteResponse.responseType());
        Assertions.assertEquals("1", E2eUtils.extractValue("RowCount", deleteResponse.response()));

        ColumnIndexManagerProvider columnIndexManagerProvider = dependencyContainer.getColumnIndexManagerProvider();

        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

        Optional<Pointer> indexPointer1 = clusterIndexManager.getIndex(1);
        Assertions.assertTrue(indexPointer1.isEmpty());
        Optional<Pointer> indexPointer2 = clusterIndexManager.getIndex(2);
        Assertions.assertTrue(indexPointer2.isEmpty());
        Optional<Pointer> indexPointer3 = clusterIndexManager.getIndex(3);
        Assertions.assertTrue(indexPointer3.isEmpty());
        Optional<Pointer> indexPointer4 = clusterIndexManager.getIndex(4);
        Assertions.assertTrue(indexPointer4.isEmpty());
        Optional<Pointer> indexPointer5 = clusterIndexManager.getIndex(5);
        Assertions.assertTrue(indexPointer5.isEmpty());
    }
}
