package org.elece.sql.parser;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.statement.ExplainStatement;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.type.Symbol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlParserTest {
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