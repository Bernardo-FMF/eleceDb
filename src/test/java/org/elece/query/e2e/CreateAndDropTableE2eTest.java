package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.query.QueryPlanner;
import org.elece.sql.parser.statement.CreateTableStatement;
import org.elece.sql.parser.statement.DropTableStatement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

/**
 * This integration test aims to validate the "create table" functionality.
 * The workflow is as follows:
 * -> If there is no table created with that name, the new table is persisted in disk, and the client receives a success message.
 * -> After dropping a table, the client receives a success message.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CreateAndDropTableE2eTest {
    private static DbConfig dbConfig;
    private static DependencyContainer dependencyContainer;

    @BeforeAll
    static void setUp() throws IOException, SchemaException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setPort(3000)
                .setBaseDbPath(Files.createTempDirectory("Create_Table_E2e_Test").toString())
                .setSessionStrategy(DbConfig.SessionStrategy.IMMEDIATE)
                .build();

        dependencyContainer = new DependencyContainer(dbConfig);

        dependencyContainer.getSchemaManager().createSchema("usersDb");
    }

    @AfterAll
    static void tearDown() throws IOException {
        FileTestUtils.deleteDirectory(dbConfig.getBaseDbPath());
    }

    @Test
    @Order(1)
    void test_createTable() throws SchemaException, ParserException, BTreeException, QueryException,
                                   SerializationException, InterruptedTaskException, StorageException,
                                   DeserializationException, ProtoException, FileChannelException, DbException,
                                   AnalyzerException, TokenizerException {
        MockedClientInterface clientInterface = new MockedClientInterface();
        QueryPlanner queryPlanner = dependencyContainer.getQueryPlanner();

        CreateTableStatement createTableStatement = (CreateTableStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), username VARCHAR(255) UNIQUE, isAdmin BOOL);");
        queryPlanner.plan(createTableStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response createTableResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.CREATE_TABLE, createTableResponse.responseType());
        Assertions.assertEquals("Table created", E2eUtils.extractValue("Message", createTableResponse.response()));
        Assertions.assertEquals("0", E2eUtils.extractValue("AffectedRowCount", createTableResponse.response()));

        Schema schema = dependencyContainer.getSchemaManager().getSchema();
        Assertions.assertNotNull(schema);
        Assertions.assertEquals("usersDb", schema.getDbName());
        Assertions.assertEquals(1, schema.getTables().size());

        Table usersTable = schema.getTables().getFirst();
        Assertions.assertEquals("users", usersTable.getName());
        Assertions.assertEquals(5, usersTable.getColumns().size());

        Column clusterIdColumn = SchemaSearcher.findClusterColumn(usersTable);
        Assertions.assertNotNull(clusterIdColumn);
        Assertions.assertEquals(CLUSTER_ID, clusterIdColumn.getName());
        Assertions.assertEquals(1, clusterIdColumn.getId());

        Optional<Column> idColumn = SchemaSearcher.findColumn(usersTable, "id");
        Assertions.assertTrue(idColumn.isPresent());
        Assertions.assertEquals("id", idColumn.get().getName());
        Assertions.assertEquals(2, idColumn.get().getId());

        Optional<Column> nameColumn = SchemaSearcher.findColumn(usersTable, "name");
        Assertions.assertTrue(nameColumn.isPresent());
        Assertions.assertEquals("name", nameColumn.get().getName());
        Assertions.assertEquals(3, nameColumn.get().getId());

        Optional<Column> usernameColumn = SchemaSearcher.findColumn(usersTable, "username");
        Assertions.assertTrue(usernameColumn.isPresent());
        Assertions.assertEquals("username", usernameColumn.get().getName());
        Assertions.assertEquals(4, usernameColumn.get().getId());

        Optional<Column> adminColumn = SchemaSearcher.findColumn(usersTable, "isAdmin");
        Assertions.assertTrue(adminColumn.isPresent());
        Assertions.assertEquals("isAdmin", adminColumn.get().getName());
        Assertions.assertEquals(5, adminColumn.get().getId());

        List<Index> indexes = usersTable.getIndexes();
        Assertions.assertEquals(3, indexes.size());
    }

    @Test
    @Order(2)
    void test_dropTable() throws SchemaException, ParserException, BTreeException, QueryException,
                                 SerializationException, InterruptedTaskException, StorageException,
                                 DeserializationException, ProtoException, FileChannelException, DbException,
                                 AnalyzerException, TokenizerException {
        MockedClientInterface clientInterface = new MockedClientInterface();
        QueryPlanner queryPlanner = dependencyContainer.getQueryPlanner();

        DropTableStatement dropTableStatement = (DropTableStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "DROP TABLE users");
        queryPlanner.plan(dropTableStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response createTableResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.DROP_TABLE, createTableResponse.responseType());
        Assertions.assertEquals("Table deleted", E2eUtils.extractValue("Message", createTableResponse.response()));
        Assertions.assertEquals("0", E2eUtils.extractValue("AffectedRowCount", createTableResponse.response()));

        Schema schema = dependencyContainer.getSchemaManager().getSchema();
        Assertions.assertNotNull(schema);
        Assertions.assertEquals("usersDb", schema.getDbName());
        Assertions.assertEquals(0, schema.getTables().size());
    }
}
