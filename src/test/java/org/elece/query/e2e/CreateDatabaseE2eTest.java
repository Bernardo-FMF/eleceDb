package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.schema.model.Schema;
import org.elece.exception.*;
import org.elece.query.QueryPlanner;
import org.elece.sql.parser.statement.CreateDbStatement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/**
 * This integration test aims to validate the "create database" functionality.
 * The workflow is as follows:
 * -> If there is no database created yet, the new database is persisted in disk, and the client receives a success message.
 * -> If a database already exists, an exception is thrown, that is then sent to the client.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CreateDatabaseE2eTest {
    private static DbConfig dbConfig;
    private static DependencyContainer dependencyContainer;

    @BeforeAll
    static void setUp() throws IOException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setPort(3000)
                .setBaseDbPath(Files.createTempDirectory("Create_Database_E2e_Test").toString())
                .setSessionStrategy(DbConfig.SessionStrategy.IMMEDIATE)
                .build();

        dependencyContainer = new DependencyContainer(dbConfig);
    }

    @AfterAll
    static void tearDown() throws IOException {
        FileTestUtils.deleteDirectory(dbConfig.getBaseDbPath());
    }

    @Test
    @Order(1)
    void test_createDatabase() throws ParserException, SchemaException, TokenizerException, AnalyzerException,
                                      BTreeException, QueryException, SerializationException, InterruptedTaskException,
                                      StorageException, DeserializationException, ProtoException, FileChannelException,
                                      DbException {
        MockedClientInterface clientInterface = new MockedClientInterface();
        QueryPlanner queryPlanner = dependencyContainer.getQueryPlanner();

        CreateDbStatement createDbStatement = (CreateDbStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(), "CREATE DATABASE usersDb");
        queryPlanner.plan(createDbStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response createDatabaseResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.CREATE_DB, createDatabaseResponse.responseType());
        Assertions.assertEquals("Database created", E2eUtils.extractValue("Message", createDatabaseResponse.response()));
        Assertions.assertEquals("0", E2eUtils.extractValue("AffectedRowCount", createDatabaseResponse.response()));

        Schema schema = dependencyContainer.getSchemaManager().getSchema();
        Assertions.assertNotNull(schema);
        Assertions.assertEquals("usersDb", schema.getDbName());
        Assertions.assertEquals(0, schema.getCollections().size());
    }

    @Test
    @Order(2)
    void test_createDatabase_failDueToDatabaseAlreadyExisting() throws SchemaException, ParserException,
                                                                       AnalyzerException, TokenizerException {
        CreateDbStatement createDbStatement = (CreateDbStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(), "CREATE DATABASE users");
        QueryPlanner queryPlanner = dependencyContainer.getQueryPlanner();
        SchemaException schemaException = Assertions.assertThrows(SchemaException.class, () -> queryPlanner.plan(createDbStatement, new MockedClientInterface()));
        Assertions.assertEquals("Database schema is already defined", schemaException.getMessage());
    }
}
