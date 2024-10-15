package org.elece.sql.analyzer;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.db.schema.model.builder.ColumnBuilder;
import org.elece.db.schema.model.builder.IndexBuilder;
import org.elece.db.schema.model.builder.SchemaBuilder;
import org.elece.db.schema.model.builder.TableBuilder;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.Statement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

class SqlAnalyzerTest {

    private static final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer();

    private static final SchemaManager schemaManager = Mockito.mock(SchemaManager.class);

    @BeforeEach
    public void setup() {
        List<Column> columns = new ArrayList<>();
        columns.add(ColumnBuilder.builder().setName("name").setSqlType(SqlType.varchar(255)).setConstraints(List.of(SqlConstraint.Unique)).build());
        columns.add(ColumnBuilder.builder().setName("id").setSqlType(SqlType.intType).setConstraints(List.of(SqlConstraint.Unique, SqlConstraint.PrimaryKey)).build());

        List<Index> indexes = new ArrayList<>();
        indexes.add(IndexBuilder.builder()
                .setName("pk_index")
                .setColumnName("id")
                .build());

        List<Table> tables = new ArrayList<>();
        tables.add(TableBuilder.builder()
                .setName("users")
                .setColumns(columns)
                .setIndexes(indexes)
                .build());

        Schema schema = SchemaBuilder.builder()
                .setDbName("userDb")
                .setTables(tables)
                .build();

        Mockito.when(schemaManager.getSchema()).thenReturn(schema);
    }

    @Test
    public void test_dropDb() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("DROP DATABASE userDb;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_dropTable() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("DROP TABLE users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_startTransaction() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("START TRANSACTION;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_rollback() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("ROLLBACK;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_commit() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("COMMIT;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_insertWithColumns() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("INSERT INTO users (id, name) VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_insertWithoutColumns() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("INSERT INTO users VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_update() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("UPDATE users SET name = \"newName\";");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_updateWhere() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("UPDATE users SET name = \"newName\" WHERE id = 1;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_createTable() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("CREATE TABLE usersAddresses (id INT PRIMARY KEY, address VARCHAR(255));");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_fail_createTable() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));");
        Statement statement = sqlParser.parse();
        Assertions.assertThrows(AnalyzerException.class, () -> sqlAnalyzer.analyze(schemaManager, statement));
    }

    @Test
    public void test_createIndex() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("CREATE UNIQUE INDEX user_name_index ON users(name);");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_fail_createIndex() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("CREATE UNIQUE INDEX id_index ON users(id);");
        Statement statement = sqlParser.parse();
        Assertions.assertThrows(AnalyzerException.class, () -> sqlAnalyzer.analyze(schemaManager, statement));
    }

    @Test
    public void test_createDb() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("CREATE DATABASE userDb;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_select() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_selectWhere() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_selectOrderBy() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users ORDER BY id;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_selectWhereOrderBy() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id > 5 ORDER BY name;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_explain() throws ParserException, TokenizerException, AnalyzerException {
        SqlParser sqlParser = new SqlParser("EXPLAIN SELECT id, name FROM users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }
}