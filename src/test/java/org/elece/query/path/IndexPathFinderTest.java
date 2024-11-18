package org.elece.query.path;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Index;
import org.elece.db.schema.model.Table;
import org.elece.db.schema.model.builder.IndexBuilder;
import org.elece.db.schema.model.builder.TableBuilder;
import org.elece.exception.ParserException;
import org.elece.exception.QueryException;
import org.elece.exception.TokenizerException;
import org.elece.query.comparator.BooleanEqualityComparator;
import org.elece.query.comparator.NumberEqualityComparator;
import org.elece.query.comparator.NumberRangeComparator;
import org.elece.query.comparator.StringEqualityComparator;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.SelectStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

class IndexPathFinderTest {
    private static final String TABLE_NAME = "user_table";
    private static final String COLUMN_ID_PRIMARY = "id";
    private static final String COLUMN_NAME_NORMAL = "name";
    private static final String COLUMN_DELETED_NORMAL = "isDeleted";

    private Table table;

    @BeforeEach
    void setup() {
        Column clusterColumn = new Column(CLUSTER_ID, SqlType.intType, new ArrayList<>(List.of(SqlConstraint.UNIQUE)));
        clusterColumn.setId(1);
        Column idColumn = new Column(COLUMN_ID_PRIMARY, SqlType.intType, new ArrayList<>(List.of(SqlConstraint.PRIMARY_KEY)));
        idColumn.setId(2);
        Column nameColumn = new Column(COLUMN_NAME_NORMAL, SqlType.varcharType, new ArrayList<>(List.of(SqlConstraint.UNIQUE)));
        nameColumn.setId(3);
        Column isDeletedColumn = new Column(COLUMN_DELETED_NORMAL, SqlType.boolType, new ArrayList<>());
        isDeletedColumn.setId(4);

        Index clusterIndex = IndexBuilder.builder()
                .setName("cluster_index")
                .setColumnName(CLUSTER_ID)
                .build();
        Index idIndex = IndexBuilder.builder()
                .setName("col_index_id")
                .setColumnName(COLUMN_ID_PRIMARY)
                .build();
        Index nameIndex = IndexBuilder.builder()
                .setName("col_index_name")
                .setColumnName(COLUMN_NAME_NORMAL)
                .build();

        table = TableBuilder.builder()
                .setName(TABLE_NAME)
                .setColumns(List.of(clusterColumn, idColumn, nameColumn, isDeletedColumn))
                .setIndexes(List.of(clusterIndex, idIndex, nameIndex))
                .build();
    }

    @Test
    void test_WhereExpressionWithSinglePath_NumberEquality() throws ParserException, TokenizerException,
                                                                    QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));
        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(1, indexPaths.size());

        IndexPath path = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY);

        Assertions.assertNotNull(path);

        Set<DefaultPathNode> nodes = path.getPathNodes();
        Assertions.assertEquals(1, nodes.size());

        DefaultPathNode idNode = findNode(nodes, COLUMN_ID_PRIMARY);

        validateNode(idNode, COLUMN_ID_PRIMARY, NumberEqualityComparator.class, 1, DefaultPathNode.IndexType.INDEXED);
    }

    @Test
    void test_WhereExpressionWithSinglePath_StringEquality() throws ParserException, TokenizerException,
                                                                    QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE name = \"user\";");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));
        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(1, indexPaths.size());

        IndexPath path = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_NAME_NORMAL);

        Assertions.assertNotNull(path);

        Set<DefaultPathNode> nodes = path.getPathNodes();
        Assertions.assertEquals(1, nodes.size());

        DefaultPathNode nameNode = findNode(nodes, COLUMN_NAME_NORMAL);

        validateNode(nameNode, COLUMN_NAME_NORMAL, StringEqualityComparator.class, "user", DefaultPathNode.IndexType.INDEXED);
    }

    @Test
    void test_WhereExpressionWithSinglePath_BooleanEquality() throws ParserException, TokenizerException,
                                                                     QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE isDeleted = false;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));
        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(1, indexPaths.size());

        IndexPath path = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_DELETED_NORMAL);

        Assertions.assertNotNull(path);

        Set<DefaultPathNode> nodes = path.getPathNodes();
        Assertions.assertEquals(1, nodes.size());

        DefaultPathNode deletedNode = findNode(nodes, COLUMN_DELETED_NORMAL);

        validateNode(deletedNode, COLUMN_DELETED_NORMAL, BooleanEqualityComparator.class, false, DefaultPathNode.IndexType.NON_INDEXED);
    }

    @Test
    void test_WhereExpressionWithUnionPaths() throws ParserException, TokenizerException, QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1 or name = \"user\";");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));
        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(2, indexPaths.size());

        IndexPath leftPath = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY);
        IndexPath rightPath = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_NAME_NORMAL);

        Assertions.assertNotNull(leftPath);
        Assertions.assertNotNull(rightPath);

        Set<DefaultPathNode> leftNodes = leftPath.getPathNodes();
        Assertions.assertEquals(1, leftNodes.size());

        Set<DefaultPathNode> rightNodes = rightPath.getPathNodes();
        Assertions.assertEquals(1, rightNodes.size());

        DefaultPathNode leftIdNode = findNode(leftNodes, COLUMN_ID_PRIMARY);

        DefaultPathNode rightNameNode = findNode(rightNodes, COLUMN_NAME_NORMAL);

        validateNode(leftIdNode, COLUMN_ID_PRIMARY, NumberEqualityComparator.class, 1, DefaultPathNode.IndexType.INDEXED);

        validateNode(rightNameNode, COLUMN_NAME_NORMAL, StringEqualityComparator.class, "user", DefaultPathNode.IndexType.INDEXED);
    }

    @Test
    void test_WhereExpressionWithMergedPaths() throws ParserException, TokenizerException, QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id = 1 and name = \"user\";");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));
        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(1, indexPaths.size());

        IndexPath path = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY, COLUMN_NAME_NORMAL);

        Assertions.assertNotNull(path);

        Set<DefaultPathNode> nodes = path.getPathNodes();
        Assertions.assertEquals(2, nodes.size());

        DefaultPathNode idNode = findNode(nodes, COLUMN_ID_PRIMARY);
        DefaultPathNode nameNode = findNode(nodes, COLUMN_NAME_NORMAL);

        validateNode(idNode, COLUMN_ID_PRIMARY, NumberEqualityComparator.class, 1, DefaultPathNode.IndexType.INDEXED);
        validateNode(nameNode, COLUMN_NAME_NORMAL, StringEqualityComparator.class, "user", DefaultPathNode.IndexType.INDEXED);
    }

    @Test
    void test_WhereExpressionWithUnionPathsMerged() throws ParserException, TokenizerException, QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE (id = 1 or name = \"user\") and isDeleted = false;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));
        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(2, indexPaths.size());

        IndexPath leftPath = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY, COLUMN_DELETED_NORMAL);
        IndexPath rightPath = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_NAME_NORMAL, COLUMN_DELETED_NORMAL);

        Assertions.assertNotNull(leftPath);
        Assertions.assertNotNull(rightPath);

        Set<DefaultPathNode> leftNodes = leftPath.getPathNodes();
        Assertions.assertEquals(2, leftNodes.size());

        Set<DefaultPathNode> rightNodes = rightPath.getPathNodes();
        Assertions.assertEquals(2, rightNodes.size());

        DefaultPathNode leftIdNode = findNode(leftNodes, COLUMN_ID_PRIMARY);
        DefaultPathNode leftDeletedNode = findNode(leftNodes, COLUMN_DELETED_NORMAL);

        DefaultPathNode rightNameNode = findNode(rightNodes, COLUMN_NAME_NORMAL);
        DefaultPathNode rightDeletedNode = findNode(rightNodes, COLUMN_DELETED_NORMAL);

        validateNode(leftIdNode, COLUMN_ID_PRIMARY, NumberEqualityComparator.class, 1, DefaultPathNode.IndexType.INDEXED);
        validateNode(leftDeletedNode, COLUMN_DELETED_NORMAL, BooleanEqualityComparator.class, false, DefaultPathNode.IndexType.NON_INDEXED);

        validateNode(rightNameNode, COLUMN_NAME_NORMAL, StringEqualityComparator.class, "user", DefaultPathNode.IndexType.INDEXED);
        validateNode(rightDeletedNode, COLUMN_DELETED_NORMAL, BooleanEqualityComparator.class, false, DefaultPathNode.IndexType.NON_INDEXED);
    }

    @Test
    void test_WhereExpressionWithMergedPathsUnion() throws ParserException, TokenizerException, QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE (id = 1 and name = \"user\") or isDeleted = false;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));

        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(2, indexPaths.size());

        IndexPath leftPath = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_DELETED_NORMAL);
        IndexPath rightPath = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY, COLUMN_NAME_NORMAL);

        Assertions.assertNotNull(leftPath);
        Assertions.assertNotNull(rightPath);

        Set<DefaultPathNode> leftNodes = leftPath.getPathNodes();
        Assertions.assertEquals(1, leftNodes.size());

        Set<DefaultPathNode> rightNodes = rightPath.getPathNodes();
        Assertions.assertEquals(2, rightNodes.size());

        DefaultPathNode leftDeletedNode = findNode(leftNodes, COLUMN_DELETED_NORMAL);

        DefaultPathNode rightIdNode = findNode(rightNodes, COLUMN_ID_PRIMARY);
        DefaultPathNode rightNameNode = findNode(rightNodes, COLUMN_NAME_NORMAL);

        validateNode(leftDeletedNode, COLUMN_DELETED_NORMAL, BooleanEqualityComparator.class, false, DefaultPathNode.IndexType.NON_INDEXED);

        validateNode(rightIdNode, COLUMN_ID_PRIMARY, NumberEqualityComparator.class, 1, DefaultPathNode.IndexType.INDEXED);
        validateNode(rightNameNode, COLUMN_NAME_NORMAL, StringEqualityComparator.class, "user", DefaultPathNode.IndexType.INDEXED);
    }

    @Test
    void test_WhereExpressionWithMergedPathsWithInvalidIntersection() throws ParserException, TokenizerException,
                                                                             QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id < 1 and id > 10;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));

        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(0, indexPaths.size());
    }

    @Test
    void test_WhereExpressionWithRange_1() throws ParserException, TokenizerException, QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id > 1;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));

        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(1, indexPaths.size());

        IndexPath path = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY);

        Assertions.assertNotNull(path);

        Set<DefaultPathNode> nodes = path.getPathNodes();
        Assertions.assertEquals(1, nodes.size());

        DefaultPathNode idNode = findNode(nodes, COLUMN_ID_PRIMARY);

        validateRangeNode(idNode, COLUMN_ID_PRIMARY, 1, Integer.MAX_VALUE, NumberRangeComparator.InclusionType.EXCLUDED, NumberRangeComparator.InclusionType.INCLUDED, DefaultPathNode.IndexType.INDEXED);
    }

    @Test
    void test_WhereExpressionWithRange_2() throws ParserException, TokenizerException, QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id <= 10;");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));

        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(1, indexPaths.size());

        IndexPath path = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY);

        Assertions.assertNotNull(path);

        Set<DefaultPathNode> nodes = path.getPathNodes();
        Assertions.assertEquals(1, nodes.size());

        DefaultPathNode idNode = findNode(nodes, COLUMN_ID_PRIMARY);

        validateRangeNode(idNode, COLUMN_ID_PRIMARY, Integer.MIN_VALUE, 10, NumberRangeComparator.InclusionType.INCLUDED, NumberRangeComparator.InclusionType.INCLUDED, DefaultPathNode.IndexType.INDEXED);
    }

    @Test
    void test_WhereExpressionWithRange_3() throws ParserException, TokenizerException, QueryException {
        SqlParser sqlParser = new SqlParser("SELECT id, name FROM users WHERE id <= 10 and name = \"user\";");
        SelectStatement statement = (SelectStatement) sqlParser.parse();

        NodeCollection nodeCollection = statement.getWhere().accept(new IndexPathFinder(table));

        Set<IndexPath> indexPaths = nodeCollection.getIndexPaths();
        Assertions.assertEquals(1, indexPaths.size());

        IndexPath path = findPathContainingColumnsAndSameSize(indexPaths, COLUMN_ID_PRIMARY, COLUMN_NAME_NORMAL);

        Assertions.assertNotNull(path);

        Set<DefaultPathNode> nodes = path.getPathNodes();
        Assertions.assertEquals(2, nodes.size());

        DefaultPathNode idNode = findNode(nodes, COLUMN_ID_PRIMARY);
        DefaultPathNode nameNode = findNode(nodes, COLUMN_NAME_NORMAL);

        validateRangeNode(idNode, COLUMN_ID_PRIMARY, Integer.MIN_VALUE, 10, NumberRangeComparator.InclusionType.INCLUDED, NumberRangeComparator.InclusionType.INCLUDED, DefaultPathNode.IndexType.INDEXED);
        validateNode(nameNode, COLUMN_NAME_NORMAL, StringEqualityComparator.class, "user", DefaultPathNode.IndexType.INDEXED);
    }

    private static IndexPath findPathContainingColumnsAndSameSize(Set<IndexPath> indexPaths, String... columns) {
        Set<String> columnsSet = new HashSet<>(Arrays.asList(columns));

        for (IndexPath indexPath : indexPaths) {
            Set<String> indexPathColumns = indexPath.getPathNodes()
                    .stream()
                    .map(DefaultPathNode::getColumnName)
                    .collect(Collectors.toSet());

            if (indexPathColumns.equals(columnsSet)) {
                return indexPath;
            }
        }

        Assertions.fail(String.format("Failed to find correspondent path with the columns %s in: %s", Arrays.toString(columns), indexPaths));

        return null;
    }

    private static <T> void validateNode(DefaultPathNode node,
                                         String expectedColumnName,
                                         Class<T> expectedComparatorClass,
                                         Object expectedComparatorValue,
                                         DefaultPathNode.IndexType expectedIndexType) {
        Assertions.assertEquals(expectedColumnName, node.getColumnName());
        Assertions.assertEquals(expectedIndexType, node.getIndexType());
        Assertions.assertInstanceOf(expectedComparatorClass, node.getValueComparator());

        T comparator = expectedComparatorClass.cast(node.getValueComparator());
        Method getBoundaryMethod = getMethodFromClassHierarchy(expectedComparatorClass, "getBoundary");
        Assertions.assertNotNull(getBoundaryMethod, "No method 'getBoundary' found in the class hierarchy");

        try {
            Object boundary = getBoundaryMethod.invoke(comparator);

            Method getValueMethod = getMethodFromClassHierarchy(boundary.getClass(), "getValue");
            Assertions.assertNotNull(getValueMethod, "No method 'getValue' found in the class hierarchy");

            Object value = getValueMethod.invoke(boundary);
            Assertions.assertEquals(expectedComparatorValue, value);
        } catch (IllegalAccessException | InvocationTargetException exception) {
            Assertions.fail("Reflection failed during boundary value validation: " + exception.getMessage());
        }
    }

    private void validateRangeNode(DefaultPathNode node, String expectedColumnName, int expectedLeftBoundary,
                                   int expectedRightBoundary,
                                   NumberRangeComparator.InclusionType expectedLeftInclusionType,
                                   NumberRangeComparator.InclusionType expectedRightInclusionType,
                                   DefaultPathNode.IndexType expectedIndexType) {
        Assertions.assertEquals(expectedColumnName, node.getColumnName());
        Assertions.assertEquals(expectedIndexType, node.getIndexType());
        Assertions.assertInstanceOf(NumberRangeComparator.class, node.getValueComparator());
        NumberRangeComparator comparator = (NumberRangeComparator) node.getValueComparator();

        Assertions.assertEquals(expectedLeftBoundary, comparator.getLeftBoundary().getValue());
        Assertions.assertEquals(expectedRightBoundary, comparator.getRightBoundary().getValue());
        Assertions.assertEquals(expectedLeftInclusionType, comparator.getLeftInclusion());
        Assertions.assertEquals(expectedRightInclusionType, comparator.getRightInclusion());
    }

    private static Method getMethodFromClassHierarchy(Class<?> clazz, String methodName) {
        Class<?> currentClass = clazz;
        while (!Objects.isNull(currentClass)) {
            try {
                return currentClass.getDeclaredMethod(methodName);
            } catch (NoSuchMethodException exception) {
                currentClass = currentClass.getSuperclass();
            }
        }

        return null;
    }

    private static DefaultPathNode findNode(Set<DefaultPathNode> nodes, String columnName) {
        Optional<DefaultPathNode> possibleNode = nodes.stream().filter(node -> node.getColumnName().equals(columnName)).findFirst();
        Assertions.assertTrue(possibleNode.isPresent());
        return possibleNode.get();
    }
}