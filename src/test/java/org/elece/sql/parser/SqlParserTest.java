package org.elece.sql.parser;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlStringValue;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.*;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.type.Symbol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlParserTest {
    @Test
    public void test_dropDb() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("DROP DATABASE userDb;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof DropDbStatement);

        DropDbStatement dropDbStatement = (DropDbStatement) statement;
        Assertions.assertEquals("userDb", dropDbStatement.getDb());
    }

    @Test
    public void test_dropTable() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("DROP TABLE users;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof DropTableStatement);

        DropTableStatement dropTableStatement = (DropTableStatement) statement;
        Assertions.assertEquals("users", dropTableStatement.getTable());
    }

    @Test
    public void test_startTransaction() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("START TRANSACTION;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof TransactionStatement);
    }

    @Test
    public void test_rollback() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("ROLLBACK;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof RollbackStatement);
    }

    @Test
    public void test_commit() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("COMMIT;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof CommitStatement);
    }

    @Test
    public void test_insertWithColumns() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("INSERT INTO users (id, name) VALUES (1, \"user1\");");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof InsertStatement);

        InsertStatement insertStatement = (InsertStatement) statement;
        Assertions.assertEquals("users", insertStatement.getTable());

        Assertions.assertEquals(2, insertStatement.getColumns().size());
        Assertions.assertEquals("id", insertStatement.getColumns().get(0));
        Assertions.assertEquals("name", insertStatement.getColumns().get(1));

        Assertions.assertEquals(new SqlNumberValue(1L), ((ValueExpression<SqlNumberValue>) insertStatement.getValues().get(0)).getValue());
        Assertions.assertEquals(new SqlStringValue("user1"), ((ValueExpression<SqlStringValue>) insertStatement.getValues().get(1)).getValue());
    }

    @Test
    public void test_insertWithoutColumns() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("INSERT INTO users VALUES (1, \"user1\");");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof InsertStatement);

        InsertStatement insertStatement = (InsertStatement) statement;
        Assertions.assertEquals("users", insertStatement.getTable());

        Assertions.assertEquals(0, insertStatement.getColumns().size());

        Assertions.assertEquals(new SqlNumberValue(1L), ((ValueExpression<SqlNumberValue>) insertStatement.getValues().get(0)).getValue());
        Assertions.assertEquals(new SqlStringValue("user1"), ((ValueExpression<SqlStringValue>) insertStatement.getValues().get(1)).getValue());
    }

    @Test
    public void test_update() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("UPDATE users SET attr = \"newAttr\";");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof UpdateStatement);

        UpdateStatement updateStatement = (UpdateStatement) statement;
        Assertions.assertEquals("users", updateStatement.getTable());
        Assertions.assertEquals(1, updateStatement.getColumns().size());
        Assertions.assertNull(updateStatement.getWhere());

        Assertions.assertEquals("attr", updateStatement.getColumns().get(0).getId());
        Assertions.assertEquals(new SqlStringValue("newAttr"), ((ValueExpression<SqlStringValue>) updateStatement.getColumns().get(0).getValue()).getValue());
    }

    @Test
    public void test_updateWhere() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("UPDATE users SET attr = \"newAttr\" WHERE id = 1;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof UpdateStatement);

        UpdateStatement updateStatement = (UpdateStatement) statement;
        Assertions.assertEquals("users", updateStatement.getTable());
        Assertions.assertEquals(1, updateStatement.getColumns().size());
        Assertions.assertNotNull(updateStatement.getWhere());

        Assertions.assertEquals("attr", updateStatement.getColumns().get(0).getId());
        Assertions.assertEquals(new SqlStringValue("newAttr"), ((ValueExpression<SqlStringValue>) updateStatement.getColumns().get(0).getValue()).getValue());

        BinaryExpression where = (BinaryExpression) updateStatement.getWhere();
        Assertions.assertEquals("id", ((IdentifierExpression) where.getLeft()).getName());
        Assertions.assertEquals(Symbol.Eq, where.getOperator());
        Assertions.assertEquals(1L, ((ValueExpression<SqlNumberValue>) where.getRight()).getValue().getValue());
    }

    @Test
    public void test_createTable() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) UNIQUE);");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof CreateTableStatement);

        CreateTableStatement createTableStatement = (CreateTableStatement) statement;
        Assertions.assertEquals("users", createTableStatement.getName());
        Assertions.assertEquals(2, createTableStatement.getColumns().size());

        Assertions.assertEquals("id", createTableStatement.getColumns().get(0).getName());
        Assertions.assertEquals(SqlType.intType, createTableStatement.getColumns().get(0).getSqlType());
        Assertions.assertEquals(1, createTableStatement.getColumns().get(0).getConstraints().size());
        Assertions.assertEquals(SqlConstraint.PrimaryKey, createTableStatement.getColumns().get(0).getConstraints().get(0));

        Assertions.assertEquals("name", createTableStatement.getColumns().get(1).getName());
        Assertions.assertEquals(SqlType.varchar(255), createTableStatement.getColumns().get(1).getSqlType());
        Assertions.assertEquals(1, createTableStatement.getColumns().get(1).getConstraints().size());
        Assertions.assertEquals(SqlConstraint.Unique, createTableStatement.getColumns().get(1).getConstraints().get(0));
    }

    @Test
    public void test_createIndex() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("CREATE INDEX user_name_index ON users(name);");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof CreateIndexStatement);

        CreateIndexStatement createIndexStatement = (CreateIndexStatement) statement;
        Assertions.assertEquals("users", createIndexStatement.getTable());
        Assertions.assertEquals("user_name_index", createIndexStatement.getName());
        Assertions.assertEquals("name", createIndexStatement.getColumn());
        Assertions.assertFalse(createIndexStatement.getUnique());
    }

    @Test
    public void test_createDb() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("CREATE DATABASE userDb;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof CreateStatement);

        CreateDbStatement createDbStatement = (CreateDbStatement) statement;
        Assertions.assertEquals("userDb", createDbStatement.getDb());
    }

    @Test
    public void test_select() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof SelectStatement);

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNull(selectStatement.getWhere());
        Assertions.assertNull(selectStatement.getOrderBy());
    }

    @Test
    public void test_selectWhere() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1;");
        Statement statement = sqlParser.parseToken().getStatement();

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNotNull(selectStatement.getWhere());
        Assertions.assertNull(selectStatement.getOrderBy());

        BinaryExpression where = (BinaryExpression) selectStatement.getWhere();
        Assertions.assertEquals("id", ((IdentifierExpression) where.getLeft()).getName());
        Assertions.assertEquals(Symbol.Eq, where.getOperator());
        Assertions.assertEquals(1L, ((ValueExpression<SqlNumberValue>) where.getRight()).getValue().getValue());
    }

    @Test
    public void test_selectOrderBy() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users ORDER BY id;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof SelectStatement);

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNull(selectStatement.getWhere());
        Assertions.assertNotNull(selectStatement.getOrderBy());
        Assertions.assertEquals(1, selectStatement.getOrderBy().size());
        Assertions.assertEquals("id", ((IdentifierExpression) selectStatement.getOrderBy().get(0)).getName());
    }

    @Test
    public void test_selectWhereOrderBy() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id > 5 ORDER BY name;");
        Statement statement = sqlParser.parseToken().getStatement();
        Assertions.assertTrue(statement instanceof SelectStatement);

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNotNull(selectStatement.getWhere());
        Assertions.assertNotNull(selectStatement.getOrderBy());
        BinaryExpression where = (BinaryExpression) selectStatement.getWhere();
        Assertions.assertEquals("id", ((IdentifierExpression) where.getLeft()).getName());
        Assertions.assertEquals(Symbol.Gt, where.getOperator());
        Assertions.assertEquals(5L, ((ValueExpression<SqlNumberValue>) where.getRight()).getValue().getValue());
        Assertions.assertNotNull(selectStatement.getOrderBy());
        Assertions.assertEquals(1, selectStatement.getOrderBy().size());
        Assertions.assertEquals("name", ((IdentifierExpression) selectStatement.getOrderBy().get(0)).getName());
    }

    @Test
    public void test_explain() throws SqlException, TokenizerException {
        ISqlParser sqlParser = new SqlParser("EXPLAIN SELECT id, name FROM users;");
        ExplainStatement statement = (ExplainStatement) sqlParser.parseToken().getStatement();

        SelectStatement selectStatement = (SelectStatement) statement.getStatement();
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNull(selectStatement.getWhere());
        Assertions.assertNull(selectStatement.getOrderBy());
    }
}