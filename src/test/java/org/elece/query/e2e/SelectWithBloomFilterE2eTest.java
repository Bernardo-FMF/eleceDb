package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SelectWithBloomFilterE2eTest {
    private static DbConfig dbConfig;
    private static DependencyContainer dependencyContainer;

    @BeforeAll
    static void setUp() throws IOException, SchemaException, ParserException, AnalyzerException, TokenizerException,
            BTreeException, QueryException, SerializationException, InterruptedTaskException,
            StorageException, DeserializationException, ProtoException, FileChannelException,
            DbException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setPort(3000)
                .setBaseDbPath(Files.createTempDirectory("Select_Bloom_E2e_Test").toString())
                .setSessionStrategy(DbConfig.SessionStrategy.IMMEDIATE)
                .setBloomFilterEnabled(true)
                .build();

        dependencyContainer = new DependencyContainer(dbConfig);

        dependencyContainer.getSchemaManager().createSchema("codesDb");

        plan("CREATE TABLE codes (id INT PRIMARY KEY, code INT UNIQUE, label VARCHAR(255) UNIQUE);");
        for (int id = 1; id <= 5; id++) {
            plan(String.format("INSERT INTO codes (id, code, label) VALUES (%d, %d, \"label%d\");", id, id * 100, id));
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        FileTestUtils.deleteDirectory(dbConfig.getBaseDbPath());
    }

    @Test
    @Order(1)
    void test_equalityOnIndexedColumn_existingValue() throws SchemaException, ParserException, BTreeException,
            QueryException, SerializationException,
            InterruptedTaskException, StorageException,
            DeserializationException, ProtoException,
            FileChannelException, DbException, AnalyzerException,
            TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, code FROM codes WHERE code = 300;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "code").get()
                ),
                List.of(
                        List.of(3, 300)
                ));
    }

    @Test
    @Order(2)
    void test_equalityOnIndexedColumn_absentValue() throws SchemaException, ParserException, BTreeException,
            QueryException, SerializationException,
            InterruptedTaskException, StorageException,
            DeserializationException, ProtoException,
            FileChannelException, DbException, AnalyzerException,
            TokenizerException {
        // The filter should report this key as absent and short circuit the tree traversal; the result must still be empty.
        planAndValidateQuery(
                "SELECT id, code FROM codes WHERE code = 999;",
                List.of(),
                List.of());
    }

    @Test
    @Order(3)
    void test_equalityOnIndexedColumn_allExistingValuesStillFound() throws SchemaException, ParserException,
            BTreeException, QueryException,
            SerializationException,
            InterruptedTaskException, StorageException,
            DeserializationException, ProtoException,
            FileChannelException, DbException,
            AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        for (int id = 1; id <= 5; id++) {
            planAndValidateQuery(
                    String.format("SELECT id, code FROM codes WHERE code = %d;", id * 100),
                    List.of(
                            SchemaSearcher.findColumn(table, "id").get(),
                            SchemaSearcher.findColumn(table, "code").get()
                    ),
                    List.of(
                            List.of(id, id * 100)
                    ));
        }
    }

    @Test
    @Order(4)
    void test_equalityOnIndexedVarcharColumn() throws SchemaException, ParserException, BTreeException, QueryException,
            SerializationException, InterruptedTaskException, StorageException,
            DeserializationException, ProtoException, FileChannelException,
            DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, label FROM codes WHERE label = \"label4\";",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "label").get()
                ),
                List.of(
                        List.of(4, "label4")
                ));
    }

    private static void plan(String sql) throws ParserException, TokenizerException, AnalyzerException, SchemaException,
            BTreeException, SerializationException, StorageException,
            DeserializationException, DbException, QueryException, ProtoException,
            InterruptedTaskException, FileChannelException {
        Statement statement = E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(), sql);
        dependencyContainer.getQueryPlanner().plan(statement, new MockedClientInterface());
    }

    private static void planAndValidateQuery(String query, List<Column> selectedColumns,
                                             List<List<Object>> expectedValues) throws SchemaException, ParserException,
            BTreeException, QueryException,
            SerializationException,
            InterruptedTaskException,
            StorageException,
            DeserializationException,
            ProtoException,
            FileChannelException,
            DbException, AnalyzerException,
            TokenizerException {
        SelectStatement selectStatement = (SelectStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(), query);
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(selectStatement, clientInterface);

        MockedClientInterface.SelectResponse selectResponse = clientInterface.getSelectResponse();
        List<List<String>> rows = selectResponse.getRows();
        Assertions.assertEquals(expectedValues.size(), rows.size());

        for (List<Object> expectedRow : expectedValues) {
            List<String> actualRow = rows.get(expectedValues.indexOf(expectedRow));

            Assertions.assertEquals(expectedRow.size(), actualRow.size());
            for (int index = 0; index < expectedRow.size(); index++) {
                Object expectedValue = expectedRow.get(index);

                Column column = selectedColumns.get(index);
                Object actualValue = actualRow.get(index);
                actualValue = switch (column.getSqlType().getType()) {
                    case INT -> Integer.parseInt(actualValue.toString());
                    case BOOL -> Boolean.parseBoolean(actualValue.toString());
                    case VARCHAR -> actualValue.toString().substring(1, actualValue.toString().length() - 1);
                };
                Assertions.assertEquals(expectedValue, actualValue);
            }
        }
    }
}
