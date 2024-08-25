package org.elece.sql.analyzer;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Index;
import org.elece.sql.db.schema.model.Schema;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.error.ParserException;
import org.elece.sql.error.TokenizerException;
import org.elece.sql.parser.ISqlParser;
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
        columns.add(new Column("name", SqlType.varchar(255), List.of()));
        columns.add(new Column("id", SqlType.intType, List.of(SqlConstraint.Unique, SqlConstraint.PrimaryKey)));

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("pk_index", "id"));

        List<Table> tables = new ArrayList<>();
        tables.add(new Table(1, "users", columns, indexes));

        Schema schema = new Schema("userDb", tables);

        Mockito.when(schemaManager.getSchema()).thenReturn(schema);
    }

    @Test
    public void test_dropDb() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("DROP DATABASE userDb;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_dropTable() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("DROP TABLE users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_startTransaction() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("START TRANSACTION;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_rollback() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("ROLLBACK;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_commit() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("COMMIT;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_insertWithColumns() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("INSERT INTO users (id, name) VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_insertWithoutColumns() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("INSERT INTO users VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_update() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("UPDATE users SET name = \"newName\";");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_updateWhere() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("UPDATE users SET name = \"newName\" WHERE id = 1;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_createTable() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("CREATE TABLE usersAddresses (id INT PRIMARY KEY, address VARCHAR(255));");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_fail_createTable() throws ParserException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));");
        Statement statement = sqlParser.parse();
        Assertions.assertThrows(AnalyzerException.class, () -> sqlAnalyzer.analyze(schemaManager, statement));
    }

    @Test
    public void test_createIndex() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("CREATE UNIQUE INDEX user_name_index ON users(name);");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_fail_createIndex() throws ParserException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("CREATE UNIQUE INDEX id_index ON users(id);");
        Statement statement = sqlParser.parse();
        Assertions.assertThrows(AnalyzerException.class, () -> sqlAnalyzer.analyze(schemaManager, statement));
    }

    @Test
    public void test_createDb() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("CREATE DATABASE userDb;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_select() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_selectWhere() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_selectOrderBy() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users ORDER BY id;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_selectWhereOrderBy() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id > 5 ORDER BY name;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }

    @Test
    public void test_explain() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("EXPLAIN SELECT id, name FROM users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
    }
}