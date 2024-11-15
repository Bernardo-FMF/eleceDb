package org.elece.sql.parser;

import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.OrderIdentifierExpression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.*;
import org.elece.sql.parser.statement.*;
import org.elece.sql.token.model.type.Symbol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlParserTest {
    @Test
    void test_dropDb() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("DROP DATABASE userDb;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(DropDbStatement.class, statement);

        DropDbStatement dropDbStatement = (DropDbStatement) statement;
        Assertions.assertEquals("userDb", dropDbStatement.getDb());
    }

    @Test
    void test_dropTable() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("DROP TABLE users;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(DropTableStatement.class, statement);

        DropTableStatement dropTableStatement = (DropTableStatement) statement;
        Assertions.assertEquals("users", dropTableStatement.getTable());
    }

    @Test
    void test_startTransaction() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("START TRANSACTION;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(TransactionStatement.class, statement);
    }

    @Test
    void test_rollback() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("ROLLBACK;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(RollbackStatement.class, statement);
    }

    @Test
    void test_commit() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("COMMIT;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(CommitStatement.class, statement);
    }

    @Test
    void test_insertWithColumns() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("INSERT INTO users (id, name) VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(InsertStatement.class, statement);

        InsertStatement insertStatement = (InsertStatement) statement;
        Assertions.assertEquals("users", insertStatement.getTable());

        Assertions.assertEquals(2, insertStatement.getColumns().size());
        Assertions.assertEquals("id", insertStatement.getColumns().get(0));
        Assertions.assertEquals("name", insertStatement.getColumns().get(1));

        Assertions.assertEquals(new SqlNumberValue(1), ((ValueExpression<SqlNumberValue>) insertStatement.getValues().get(0)).getValue());
        Assertions.assertEquals(new SqlStringValue("user1"), ((ValueExpression<SqlStringValue>) insertStatement.getValues().get(1)).getValue());
    }

    @Test
    void test_insertWithoutColumns() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("INSERT INTO users VALUES (1, \"user1\");");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(InsertStatement.class, statement);

        InsertStatement insertStatement = (InsertStatement) statement;
        Assertions.assertEquals("users", insertStatement.getTable());

        Assertions.assertEquals(0, insertStatement.getColumns().size());

        Assertions.assertEquals(new SqlNumberValue(1), ((ValueExpression<SqlNumberValue>) insertStatement.getValues().get(0)).getValue());
        Assertions.assertEquals(new SqlStringValue("user1"), ((ValueExpression<SqlStringValue>) insertStatement.getValues().get(1)).getValue());
    }

    @Test
    void test_update() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("UPDATE users SET attr = \"newAttr\";");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(UpdateStatement.class, statement);

        UpdateStatement updateStatement = (UpdateStatement) statement;
        Assertions.assertEquals("users", updateStatement.getTable());
        Assertions.assertEquals(1, updateStatement.getColumns().size());
        Assertions.assertNull(updateStatement.getWhere());

        Assertions.assertEquals("attr", updateStatement.getColumns().get(0).getId());
        Assertions.assertEquals(new SqlStringValue("newAttr"), ((ValueExpression<SqlStringValue>) updateStatement.getColumns().get(0).getValue()).getValue());
    }

    @Test
    void test_updateWhere() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("UPDATE users SET attr = \"newAttr\" WHERE id = 1;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(UpdateStatement.class, statement);

        UpdateStatement updateStatement = (UpdateStatement) statement;
        Assertions.assertEquals("users", updateStatement.getTable());
        Assertions.assertEquals(1, updateStatement.getColumns().size());
        Assertions.assertNotNull(updateStatement.getWhere());

        Assertions.assertEquals("attr", updateStatement.getColumns().get(0).getId());
        Assertions.assertEquals(new SqlStringValue("newAttr"), ((ValueExpression<SqlStringValue>) updateStatement.getColumns().get(0).getValue()).getValue());

        BinaryExpression where = (BinaryExpression) updateStatement.getWhere();
        Assertions.assertEquals("id", ((IdentifierExpression) where.getLeft()).getName());
        Assertions.assertEquals(Symbol.Eq, where.getOperator());
        Assertions.assertEquals(1, ((ValueExpression<SqlNumberValue>) where.getRight()).getValue().getValue());
    }

    @Test
    void test_createTable() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) UNIQUE);");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(CreateTableStatement.class, statement);

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
    void test_createIndex() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("CREATE INDEX user_name_index ON users(name);");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(CreateIndexStatement.class, statement);

        CreateIndexStatement createIndexStatement = (CreateIndexStatement) statement;
        Assertions.assertEquals("users", createIndexStatement.getTable());
        Assertions.assertEquals("user_name_index", createIndexStatement.getName());
        Assertions.assertEquals("name", createIndexStatement.getColumn());
        Assertions.assertFalse(createIndexStatement.getUnique());
    }

    @Test
    void test_createDb() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("CREATE DATABASE userDb;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(CreateStatement.class, statement);

        CreateDbStatement createDbStatement = (CreateDbStatement) statement;
        Assertions.assertEquals("userDb", createDbStatement.getDb());
    }

    @Test
    void test_select() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(SelectStatement.class, statement);

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNull(selectStatement.getWhere());
        Assertions.assertEquals(0, selectStatement.getOrderBy().size());
    }

    @Test
    void test_selectWhere() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1;");
        Statement statement = sqlParser.parse();

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNotNull(selectStatement.getWhere());
        Assertions.assertEquals(0, selectStatement.getOrderBy().size());

        BinaryExpression where = (BinaryExpression) selectStatement.getWhere();
        Assertions.assertEquals("id", ((IdentifierExpression) where.getLeft()).getName());
        Assertions.assertEquals(Symbol.Eq, where.getOperator());
        Assertions.assertEquals(1, ((ValueExpression<SqlNumberValue>) where.getRight()).getValue().getValue());
    }

    @Test
    void test_selectOrderBy() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users ORDER BY id;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(SelectStatement.class, statement);

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNull(selectStatement.getWhere());
        Assertions.assertNotNull(selectStatement.getOrderBy());
        Assertions.assertEquals(1, selectStatement.getOrderBy().size());
        Assertions.assertEquals("id", ((OrderIdentifierExpression) selectStatement.getOrderBy().get(0)).getName());
        Assertions.assertEquals(Order.DEFAULT_ORDER, ((OrderIdentifierExpression) selectStatement.getOrderBy().get(0)).getOrder());
    }

    @Test
    void test_selectWhereOrderBy() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id > 5 ORDER BY name;");
        Statement statement = sqlParser.parse();
        Assertions.assertInstanceOf(SelectStatement.class, statement);

        SelectStatement selectStatement = (SelectStatement) statement;
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNotNull(selectStatement.getWhere());
        Assertions.assertNotNull(selectStatement.getOrderBy());
        BinaryExpression where = (BinaryExpression) selectStatement.getWhere();
        Assertions.assertEquals("id", ((IdentifierExpression) where.getLeft()).getName());
        Assertions.assertEquals(Symbol.Gt, where.getOperator());
        Assertions.assertEquals(5, ((ValueExpression<SqlNumberValue>) where.getRight()).getValue().getValue());
        Assertions.assertNotNull(selectStatement.getOrderBy());
        Assertions.assertEquals(1, selectStatement.getOrderBy().size());
        Assertions.assertEquals("name", ((OrderIdentifierExpression) selectStatement.getOrderBy().get(0)).getName());
        Assertions.assertEquals(Order.DEFAULT_ORDER, ((OrderIdentifierExpression) selectStatement.getOrderBy().get(0)).getOrder());
    }

    @Test
    void test_explain() throws ParserException, TokenizerException {
        SqlParser sqlParser = new SqlParser("EXPLAIN SELECT id, name FROM users;");
        ExplainStatement statement = (ExplainStatement) sqlParser.parse();

        SelectStatement selectStatement = (SelectStatement) statement.getStatement();
        Assertions.assertEquals("users", selectStatement.getFrom());
        Assertions.assertNull(selectStatement.getWhere());
        Assertions.assertEquals(0, selectStatement.getOrderBy().size());
    }
}