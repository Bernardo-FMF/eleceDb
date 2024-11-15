package org.elece.query.e2e;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.CreateTableStatement;
import org.elece.sql.parser.statement.InsertStatement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.FileTestUtils;
import org.elece.utils.SerializationUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class InsertE2eTest {
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

        CreateTableStatement createTableStatement = (CreateTableStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), username VARCHAR(255) UNIQUE, isAdmin BOOL);");
        dependencyContainer.getQueryPlanner().plan(createTableStatement, new MockedClientInterface());
    }

    @AfterAll
    static void tearDown() throws IOException {
        FileTestUtils.deleteDirectory(dbConfig.getBaseDbPath());
    }

    @Test
    void test_insertRow() throws SchemaException, InterruptedTaskException, StorageException, FileChannelException,
                                 DbException, ParserException, AnalyzerException, TokenizerException, BTreeException,
                                 QueryException, SerializationException, DeserializationException, ProtoException {
        InsertStatement insertStatement = (InsertStatement) E2eUtils.prepareStatement(dependencyContainer.getSchemaManager(),
                "INSERT INTO users (id, name, username, isAdmin) VALUES (1, \"name\", \"username\", true);");
        MockedClientInterface clientInterface = new MockedClientInterface();
        dependencyContainer.getQueryPlanner().plan(insertStatement, clientInterface);

        List<MockedClientInterface.Response> responses = clientInterface.getResponses();
        Assertions.assertEquals(1, responses.size());

        MockedClientInterface.Response insertResponse = responses.getFirst();
        Assertions.assertEquals(MockedClientInterface.ResponseType.INSERT, insertResponse.responseType());
        Assertions.assertEquals("1", E2eUtils.extractValue("RowCount", insertResponse.response()));

        ColumnIndexManagerProvider columnIndexManagerProvider = dependencyContainer.getColumnIndexManagerProvider();
        DatabaseStorageManager databaseStorageManager = dependencyContainer.getDatabaseStorageManager();

        Table table = dependencyContainer.getSchemaManager().getSchema().getTables().getFirst();
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        Optional<Pointer> indexPointer = clusterIndexManager.getIndex(1);
        Assertions.assertTrue(indexPointer.isPresent());

        Optional<DbObject> optionalDbObject = databaseStorageManager.select(indexPointer.get());
        Assertions.assertTrue(optionalDbObject.isPresent());

        DbObject dbObject = optionalDbObject.get();

        SerializerRegistry serializerRegistry = dependencyContainer.getSerializerRegistry();
        Serializer<Integer> intSerializer = serializerRegistry.getSerializer(SqlType.Type.Int);
        Serializer<String> varcharSerializer = serializerRegistry.getSerializer(SqlType.Type.Varchar);
        Serializer<Boolean> boolSerializer = serializerRegistry.getSerializer(SqlType.Type.Bool);
        byte[] clusterValueBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findClusterColumn(table), dbObject);
        byte[] idValueBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findColumn(table, "id").get(), dbObject);
        byte[] nameValueBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findColumn(table, "name").get(), dbObject);
        byte[] usernameValueBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findColumn(table, "username").get(), dbObject);
        byte[] isAdminValueBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findColumn(table, "isAdmin").get(), dbObject);

        Assertions.assertEquals(1, intSerializer.deserialize(clusterValueBytes, SchemaSearcher.findClusterColumn(table)));
        Assertions.assertEquals(1, intSerializer.deserialize(idValueBytes, SchemaSearcher.findColumn(table, "id").get()));
        Assertions.assertEquals("name", varcharSerializer.deserialize(nameValueBytes, SchemaSearcher.findColumn(table, "name").get()).trim());
        Assertions.assertEquals("username", varcharSerializer.deserialize(usernameValueBytes, SchemaSearcher.findColumn(table, "username").get()).trim());
        Assertions.assertTrue(boolSerializer.deserialize(isAdminValueBytes, SchemaSearcher.findColumn(table, "isAdmin").get()));
    }
}
