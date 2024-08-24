package org.elece.sql.optimizer;

import org.elece.sql.analyzer.SqlAnalyzer;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Index;
import org.elece.sql.db.schema.model.Schema;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.error.ParserException;
import org.elece.sql.error.TokenizerException;
import org.elece.sql.parser.ISqlParser;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.SelectStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

class SqlOptimizerTest {
    private static final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer();
    private static final SqlOptimizer sqlOptimizer = new SqlOptimizer();

    private static final SchemaManager schemaManager = Mockito.mock(SchemaManager.class);

    @BeforeEach
    public void setup() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("name", SqlType.varchar(255), List.of()));
        columns.add(new Column("id", SqlType.intType, List.of(SqlConstraint.Unique, SqlConstraint.PrimaryKey)));

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("pk_index", "id"));

        List<Collection> collections = new ArrayList<>();
        collections.add(new Collection(1, "users", columns, indexes));

        Schema schema = new Schema("userDb", collections);

        Mockito.when(schemaManager.getSchema()).thenReturn(schema);
    }

    @Test
    public void test_selectWhere_sumZero() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id + 0 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
        sqlOptimizer.optimize(schemaManager, statement);

        Assertions.assertTrue(((BinaryExpression) statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_subtractZero() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id - 0 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
        sqlOptimizer.optimize(schemaManager, statement);

        Assertions.assertTrue(((BinaryExpression) statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_divOne() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id / 1 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
        sqlOptimizer.optimize(schemaManager, statement);

        Assertions.assertTrue(((BinaryExpression) statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_mulOne() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id * 1 = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
        sqlOptimizer.optimize(schemaManager, statement);

        Assertions.assertTrue(((BinaryExpression) statement.getWhere()).getLeft() instanceof IdentifierExpression);
    }

    @Test
    public void test_selectWhere_simplifySum() throws ParserException, TokenizerException, AnalyzerException {
        ISqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id + 2 + 4 > 8;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();
        sqlAnalyzer.analyze(schemaManager, statement);
        sqlOptimizer.optimize(schemaManager, statement);

        Assertions.assertTrue(((BinaryExpression) statement.getWhere()).getLeft() instanceof BinaryExpression);
        Assertions.assertTrue(((BinaryExpression) ((BinaryExpression) statement.getWhere()).getLeft()).getLeft() instanceof IdentifierExpression);
        Assertions.assertTrue(((BinaryExpression) ((BinaryExpression) statement.getWhere()).getLeft()).getRight() instanceof ValueExpression<?>);
        Assertions.assertEquals(6, ((ValueExpression<?>) ((BinaryExpression) ((BinaryExpression) statement.getWhere()).getLeft()).getRight()).getValue().getValue());
    }
}