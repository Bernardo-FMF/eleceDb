package org.elece.sql.analyzer;

import org.elece.sql.error.AnalyzerException;
import org.elece.sql.db.*;
import org.elece.sql.parser.ISqlParser;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.expression.internal.Column;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.error.TokenizerException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class SqlAnalyzerTest {

    private static final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer();

    private static final IContext<String, TableMetadata> context = new DbContext();

    static {
        Column nameColumn = new Column("name", SqlType.varchar(255), List.of());
        Column idColumn = new Column("id", SqlType.intType, List.of(SqlConstraint.Unique, SqlConstraint.PrimaryKey));
        Schema schema = new Schema(List.of(idColumn, nameColumn));
        TableMetadata userTable = new TableMetadata(0, "users", schema, List.of(new IndexMetadata(0, "id_index", idColumn, schema, true)), 0L);
        context.insert("users", userTable);
    }

    @Test
    public void test_dropDb() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("DROP DATABASE userDb;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_dropTable() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("DROP TABLE users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_startTransaction() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("START TRANSACTION;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_rollback() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("ROLLBACK;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_commit() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("COMMIT;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_insertWithColumns() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("INSERT INTO users (id, name) VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_insertWithoutColumns() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("INSERT INTO users VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_update() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("UPDATE users SET name = \"newName\";");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_updateWhere() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("UPDATE users SET name = \"newName\" WHERE id = 1;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_createTable() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("CREATE TABLE usersAddresses (id INT PRIMARY KEY, address VARCHAR(255));");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_fail_createTable() throws ParserException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));");
        Statement statement = sqlParser.parse();
        Assertions.assertThrows(AnalyzerException.class, () -> sqlAnalyzer.analyze(context, statement));
    }

    @Test
    public void test_createIndex() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("CREATE UNIQUE INDEX user_name_index ON users(name);");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_fail_createIndex() throws ParserException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("CREATE UNIQUE INDEX id_index ON users(id);");
        Statement statement = sqlParser.parse();
        Assertions.assertThrows(AnalyzerException.class, () -> sqlAnalyzer.analyze(context, statement));
    }

    @Test
    public void test_createDb() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("CREATE DATABASE userDb;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_select() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_selectWhere() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_selectOrderBy() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users ORDER BY id;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_selectWhereOrderBy() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id > 5 ORDER BY name;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }

    @Test
    public void test_explain() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("EXPLAIN SELECT id, name FROM users;");
        Statement statement = sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
    }
}