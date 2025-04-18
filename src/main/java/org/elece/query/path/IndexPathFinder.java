package org.elece.query.path;

import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.QueryException;
import org.elece.query.comparator.*;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.NestedExpression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.SqlBoolValue;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlStringValue;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexPathFinder implements QueryPlanVisitor {
    private static final EnumSet<Symbol> rangeSymbols = EnumSet.of(Symbol.EQ, Symbol.NEQ, Symbol.LT, Symbol.LT_EQ, Symbol.GT, Symbol.GT_EQ);

    private final Table table;

    public IndexPathFinder(Table table) {
        this.table = table;
    }

    @Override
    public NodeCollection visit(BinaryExpression binaryExpression) throws QueryException {
        NodeCollection nodeCollection = new NodeCollection();

        if (isValueComparison(binaryExpression)) {
            IdentifierExpression identifierExpression = (IdentifierExpression) (binaryExpression.getLeft() instanceof IdentifierExpression ? binaryExpression.getLeft() : binaryExpression.getRight());

            if (rangeSymbols.contains((Symbol) binaryExpression.getOperator())) {
                boolean valueIsLeftSide = (binaryExpression.getLeft() instanceof ValueExpression<?>);
                ValueExpression<?> valueExpression = (ValueExpression<?>) (binaryExpression.getLeft() instanceof ValueExpression<?> ? binaryExpression.getLeft() : binaryExpression.getRight());

                ValueComparator<?> valueComparator = determineBounds((Symbol) binaryExpression.getOperator(), valueExpression, valueIsLeftSide);

                DefaultPathNode defaultPathNode = new DefaultPathNode(identifierExpression.getName(), valueComparator, DefaultPathNode.IndexType.fromBoolean(SchemaSearcher.columnIsIndexed(table, identifierExpression.getName())));
                IndexPath indexPath = new IndexPath();
                indexPath.addPath(defaultPathNode);

                nodeCollection.addPath(indexPath);

                return nodeCollection;
            }
        }

        boolean isAndOperator = binaryExpression.getOperator() instanceof Keyword keyword && keyword == Keyword.AND;
        boolean isOrOperator = binaryExpression.getOperator() instanceof Keyword keyword && keyword == Keyword.OR;

        if (isAndOperator || isOrOperator) {
            NodeCollection leftPaths = binaryExpression.getLeft().accept(this);
            NodeCollection rightPaths = binaryExpression.getRight().accept(this);

            if (isAndOperator) {
                return processAndOperator(leftPaths, rightPaths);
            } else {
                return processOrOperator(leftPaths, rightPaths);
            }
        }

        // If the binary expression is not a value comparison, nor an and/or expression,
        // then we can assume that it's actually a complex expression like 'id = value + 1'
        IndexPath indexPath = new IndexPath();
        indexPath.addPath(new ComplexPathNode(binaryExpression));
        nodeCollection.addPath(indexPath);

        return nodeCollection;
    }

    @Override
    public NodeCollection visit(NestedExpression nestedExpression) throws QueryException {
        return nestedExpression.getExpression().accept(this);
    }

    private NodeCollection processAndOperator(NodeCollection leftPaths,
                                              NodeCollection rightPaths) {
        if (leftPaths.isEmpty() && rightPaths.isEmpty()) {
            return leftPaths;
        }

        if (leftPaths.isEmpty()) {
            return rightPaths;
        } else if (rightPaths.isEmpty()) {
            return leftPaths;
        } else {
            for (IndexPath rightPath : rightPaths.getIndexPaths()) {
                leftPaths.mergePath(rightPath);
            }
        }

        Set<IndexPath> indexPaths = leftPaths.getIndexPaths();
        Set<IndexPath> mergedIndexPaths = new HashSet<>();

        AtomicBoolean isCanceled = new AtomicBoolean(false);
        for (IndexPath indexPath : indexPaths) {
            Map<String, List<DefaultPathNode>> leftNodesGroupedByColumn = new HashMap<>();

            for (DefaultPathNode node : indexPath.getPathNodes()) {
                if (leftNodesGroupedByColumn.containsKey(node.getColumnName())) {
                    leftNodesGroupedByColumn.get(node.getColumnName()).add(node);
                } else {
                    leftNodesGroupedByColumn.put(node.getColumnName(), new ArrayList<>(List.of(node)));
                }
            }

            IndexPath mergedIndexPath = new IndexPath();
            indexPath.getComplexPathNodes().forEach(mergedIndexPath::addPath);
            for (Map.Entry<String, List<DefaultPathNode>> columnEntry : leftNodesGroupedByColumn.entrySet()) {
                if (columnEntry.getValue().size() == 1) {
                    mergedIndexPath.addPath(columnEntry.getValue().getFirst());
                } else {
                    Optional<DefaultPathNode> possibleMergedIntersection = mergeIntersection(columnEntry.getValue());
                    possibleMergedIntersection.ifPresentOrElse(mergedIndexPath::addPath, () -> isCanceled.set(true));
                }
            }

            mergedIndexPaths.add(mergedIndexPath);
        }


        if (isCanceled.get()) {
            return new NodeCollection();
        }

        NodeCollection mergedNodes = new NodeCollection();
        mergedIndexPaths.forEach(mergedNodes::addPath);
        return mergedNodes;
    }

    private NodeCollection processOrOperator(NodeCollection leftPaths, NodeCollection rightPaths) {
        if (leftPaths.isEmpty() && rightPaths.isEmpty()) {
            return leftPaths;
        }

        for (IndexPath rightPath : rightPaths.getIndexPaths()) {
            leftPaths.addPath(rightPath);
        }

        return leftPaths;
    }

    private <V> Optional<DefaultPathNode> mergeIntersection(List<DefaultPathNode> nodes) {
        if (nodes.isEmpty()) {
            return Optional.empty();
        }

        if (nodes.size() == 1) {
            return Optional.of(nodes.getFirst());
        }

        List<ValueComparator<V>> comparators = nodes.stream().map(node -> (ValueComparator<V>) node.getValueComparator()).toList();
        ValueComparator<V> initialComparator = comparators.getFirst();
        for (int index = 1; index < comparators.size(); index++) {
            ValueComparator<V> currentComparator = comparators.get(index);

            Optional<ValueComparator<V>> mergedComparator = initialComparator.intersect(currentComparator);
            if (mergedComparator.isEmpty()) {
                return Optional.empty();
            }

            initialComparator = mergedComparator.get();
        }

        DefaultPathNode newNode = new DefaultPathNode(nodes.getFirst().getColumnName(), initialComparator, DefaultPathNode.IndexType.fromBoolean(SchemaSearcher.columnIsIndexed(table, nodes.getFirst().getColumnName())));
        return Optional.of(newNode);
    }

    private static boolean isValueComparison(BinaryExpression binaryExpression) {
        return (binaryExpression.getLeft() instanceof IdentifierExpression && binaryExpression.getRight() instanceof ValueExpression<?>) ||
                (binaryExpression.getRight() instanceof IdentifierExpression && binaryExpression.getLeft() instanceof ValueExpression<?>);
    }

    private static ValueComparator<?> determineBounds(Symbol operator, ValueExpression<?> valueExpression,
                                                      boolean valueIsLeftSide) {
        if (operator == Symbol.EQ || operator == Symbol.NEQ) {
            return buildEqualityComparator(valueExpression, operator == Symbol.EQ);
        }

        boolean isLeftBoundary = (valueIsLeftSide && (operator == Symbol.LT || operator == Symbol.LT_EQ)) || (!valueIsLeftSide && (operator == Symbol.GT || operator == Symbol.GT_EQ));
        return buildRangeComparator((ValueExpression<SqlNumberValue>) valueExpression, operator == Symbol.LT_EQ || operator == Symbol.GT_EQ, isLeftBoundary);
    }

    private static NumberRangeComparator buildRangeComparator(ValueExpression<SqlNumberValue> valueExpression,
                                                              boolean inclusive,
                                                              boolean valueIsLeftBoundary) {
        SqlNumberValue leftValue = valueIsLeftBoundary ? valueExpression.getValue() : null;
        SqlNumberValue rightValue = !valueIsLeftBoundary ? valueExpression.getValue() : null;

        NumberRangeComparator.InclusionType inclusionType = inclusive ? NumberRangeComparator.InclusionType.INCLUDED : NumberRangeComparator.InclusionType.EXCLUDED;

        return new NumberRangeComparator(leftValue, rightValue, inclusionType, inclusionType);
    }

    private static EqualityComparator<?> buildEqualityComparator(ValueExpression<?> valueExpression,
                                                                 boolean shouldBeEqual) {
        SqlValue<?> value = valueExpression.getValue();
        return switch (value) {
            case SqlNumberValue numberValue -> new NumberEqualityComparator(numberValue, shouldBeEqual);
            case SqlStringValue stringValue -> new StringEqualityComparator(stringValue, shouldBeEqual);
            default -> new BooleanEqualityComparator((SqlBoolValue) value, shouldBeEqual);
        };
    }
}
