package org.elece.sql.optimizer;

import org.elece.sql.analyzer.SqlAnalyzer;
import org.elece.sql.db.*;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.error.ParserException;
import org.elece.sql.error.TokenizerException;
import org.elece.sql.parser.ISqlParser;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.Column;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.SelectStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

class SqlOptimizerTest {
    private static final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer();
    private static final SqlOptimizer sqlOptimizer = new SqlOptimizer();

    private static final IContext<String, TableMetadata> context = new DbContext();

    static {
        Column nameColumn = new Column("name", SqlType.varchar(255), List.of());
        Column idColumn = new Column("id", SqlType.intType, List.of(SqlConstraint.Unique, SqlConstraint.PrimaryKey));
        Schema schema = new Schema(List.of(idColumn, nameColumn));
        TableMetadata userTable = new TableMetadata(0, "users", schema, List.of(new IndexMetadata(0, "id_index", idColumn, schema, true)), 0L);
        context.insert("users", userTable);
    }

    @Test
    public void test_selectWhere_sumZero() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id + 0 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
        sqlOptimizer.optimize(statement);

        Assertions.assertTrue(((BinaryExpression)statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_subtractZero() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id - 0 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
        sqlOptimizer.optimize(statement);

        Assertions.assertTrue(((BinaryExpression)statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_divOne() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id / 1 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
        sqlOptimizer.optimize(statement);

        Assertions.assertTrue(((BinaryExpression)statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_mulOne() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id * 1 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
        sqlOptimizer.optimize(statement);

        Assertions.assertTrue(((BinaryExpression)statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_simplifySum() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id + 2 + 4 > 8;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(context, statement);
        sqlOptimizer.optimize(statement);

        Assertions.assertTrue(((BinaryExpression)statement.getWhere()).getLeft() instanceof BinaryExpression);
        Assertions.assertTrue(((BinaryExpression) ((BinaryExpression)statement.getWhere()).getLeft()).getLeft() instanceof IdentifierExpression);
        Assertions.assertTrue(((BinaryExpression) ((BinaryExpression)statement.getWhere()).getLeft()).getRight() instanceof ValueExpression<?>);
        Assertions.assertEquals(new BigInteger("6"), ((ValueExpression<?>) ((BinaryExpression) ((BinaryExpression)statement.getWhere()).getLeft()).getRight()).getValue().getValue());
    }
}