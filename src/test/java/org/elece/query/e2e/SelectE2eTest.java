package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SelectE2eTest {
    private static DbConfig dbConfig;
    private static DependencyContainer dependencyContainer;

    @BeforeAll
    static void setUp() throws IOException, SchemaException, ParserException, AnalyzerException, TokenizerException,
                               BTreeException, QueryException, SerializationException, InterruptedTaskException,
                               StorageException, DeserializationException, ProtoException, FileChannelException,
                               DbException {
        dbConfig = DefaultDbConfigBuilder.builder()
                .setPort(3000)
                .setBaseDbPath(Files.createTempDirectory("Select_E2e_Test").toString())
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
    void test_selectWithPrimaryKeyWhere_allColumns() throws SchemaException, ParserException, BTreeException,
                                                            QueryException,
                                                            SerializationException, InterruptedTaskException,
                                                            StorageException,
                                                            DeserializationException, ProtoException,
                                                            FileChannelException,
                                                            DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT * FROM users WHERE id = 1;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "name").get(),
                        SchemaSearcher.findColumn(table, "username").get(),
                        SchemaSearcher.findColumn(table, "isAdmin").get(),
                        SchemaSearcher.findColumn(table, "numberColumn").get()
                ),
                List.of(
                        List.of(1, "name1", "username1", true, 1)
                ));
    }

    @Test
    @Order(2)
    void test_selectWithPrimaryKeyWhere_partialColumns() throws SchemaException, ParserException, BTreeException,
                                                                QueryException,
                                                                SerializationException, InterruptedTaskException,
                                                                StorageException,
                                                                DeserializationException, ProtoException,
                                                                FileChannelException,
                                                                DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, isAdmin FROM users WHERE id = 1;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "isAdmin").get()
                ),
                List.of(
                        List.of(1, true)
                ));
    }

    @Test
    @Order(3)
    void test_selectWithNonIndexedWhere_ascending() throws SchemaException, ParserException, BTreeException,
                                                           QueryException,
                                                           SerializationException, InterruptedTaskException,
                                                           StorageException,
                                                           DeserializationException, ProtoException,
                                                           FileChannelException,
                                                           DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, name FROM users WHERE name = \"name1\" OR name = \"name2\" ORDER BY id ASC;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "name").get()
                ),
                List.of(
                        List.of(1, "name1"),
                        List.of(2, "name2")
                ));
    }

    @Test
    @Order(4)
    void test_selectWithNonIndexedWhere_descending() throws SchemaException, ParserException, BTreeException,
                                                            QueryException,
                                                            SerializationException, InterruptedTaskException,
                                                            StorageException,
                                                            DeserializationException, ProtoException,
                                                            FileChannelException,
                                                            DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, name FROM users WHERE name = \"name1\" OR name = \"name2\" ORDER BY id DESC;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "name").get()
                ),
                List.of(
                        List.of(2, "name2"),
                        List.of(1, "name1")
                ));
    }

    @Test
    @Order(5)
    void test_selectWithoutWhere() throws SchemaException, ParserException, BTreeException,
                                          QueryException,
                                          SerializationException, InterruptedTaskException,
                                          StorageException,
                                          DeserializationException, ProtoException,
                                          FileChannelException,
                                          DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, name FROM users;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "name").get()
                ),
                List.of(
                        List.of(1, "name1"),
                        List.of(2, "name2"),
                        List.of(3, "name3"),
                        List.of(4, "name4"),
                        List.of(5, "name5")
                ));
    }

    @Test
    @Order(6)
    void test_selectWithIndexedWhere() throws SchemaException, ParserException, BTreeException,
                                              QueryException,
                                              SerializationException, InterruptedTaskException,
                                              StorageException,
                                              DeserializationException, ProtoException,
                                              FileChannelException,
                                              DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, name FROM users WHERE username != \"username1\";",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "name").get()
                ),
                List.of(
                        List.of(2, "name2"),
                        List.of(3, "name3"),
                        List.of(4, "name4"),
                        List.of(5, "name5")
                ));
    }

    @Test
    @Order(7)
    void test_selectWithComplexVariableWhere() throws SchemaException, ParserException, BTreeException,
                                                      QueryException,
                                                      SerializationException, InterruptedTaskException,
                                                      StorageException,
                                                      DeserializationException, ProtoException,
                                                      FileChannelException,
                                                      DbException, AnalyzerException, TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, name FROM users WHERE id = numberColumn;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "name").get()
                ),
                List.of(
                        List.of(1, "name1"),
                        List.of(2, "name2"),
                        List.of(3, "name3")
                ));
    }

    @Test
    @Order(8)
    void test_selectWithComplexVariableWhereWithMultipleBranches() throws SchemaException, ParserException,
                                                                          BTreeException,
                                                                          QueryException,
                                                                          SerializationException,
                                                                          InterruptedTaskException,
                                                                          StorageException,
                                                                          DeserializationException, ProtoException,
                                                                          FileChannelException,
                                                                          DbException, AnalyzerException,
                                                                          TokenizerException {
        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        planAndValidateQuery(
                "SELECT id, name FROM users WHERE id = numberColumn and isAdmin = true;",
                List.of(
                        SchemaSearcher.findColumn(table, "id").get(),
                        SchemaSearcher.findColumn(table, "name").get()
                ),
                List.of(
                        List.of(1, "name1")
                ));
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
