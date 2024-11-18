package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.sql.parser.statement.CreateIndexStatement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CreateIndexE2eTest {
    private static DbConfig dbConfig;
    private static DependencyContainer dependencyContainer;

    @BeforeAll
    static void setUp() throws IOException, SchemaException, ParserException, AnalyzerException, TokenizerException,
                               BTreeException, QueryException, SerializationException, InterruptedTaskException,
                               StorageException, DeserializationException, ProtoException, FileChannelException,
                               DbException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setPort(3000)
                .setBaseDbPath(Files.createTempDirectory("Create_Index_E2e_Test").toString())
                .setSessionStrategy(DbConfig.SessionStrategy.IMMEDIATE)
                .build();

        dependencyContainer = new DependencyContainer(dbConfig);

        dependencyContainer.getSchemaManager().createSchema("usersDb");

        E2eUtils.createTable(dependencyContainer);
    }

    @AfterAll
    static void tearDown() throws IOException {
        FileTestUtils.deleteDirectory(dbConfig.getBaseDbPath());
    }

    @Test
    void test_createIndex() throws SchemaException, InterruptedTaskException, StorageException, FileChannelException,
                                   DbException, ParserException, AnalyzerException, TokenizerException, BTreeException,
                                   QueryException, SerializationException, DeserializationException, ProtoException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        Object[] rowData = new Object[]{1, 1, "name", "userUniqueName", true, 1};

        byte[] row = E2eUtils.serializeRow(table, rowData);

        E2eUtils.insertRow(dependencyContainer, table, row, rowData);

        CreateIndexStatement createIndexStatement = (CreateIndexStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "CREATE UNIQUE INDEX name_index ON users(name);");
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(createIndexStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response createIndexResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.CREATE_INDEX, createIndexResponse.responseType());
        Assertions.assertEquals("Index created", E2eUtils.extractValue("Message", createIndexResponse.response()));
        Assertions.assertEquals("1", E2eUtils.extractValue("RowCount", createIndexResponse.response()));
    }
}
