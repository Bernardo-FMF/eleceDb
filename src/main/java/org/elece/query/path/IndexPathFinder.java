package org.elece.query.path;

import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.query.QueryException;
import org.elece.exception.sql.ParserException;
import org.elece.query.QueryPlanVisitor;
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

public class IndexPathFinder implements QueryPlanVisitor {
    private static final EnumSet<Symbol> rangeSymbols = EnumSet.of(Symbol.Eq, Symbol.Neq, Symbol.Lt, Symbol.LtEq, Symbol.Gt, Symbol.GtEq);

    private final Table table;
    private final Set<String> canceledColumnPaths;

    public IndexPathFinder(Table table) {
        this.table = table;
        this.canceledColumnPaths = new HashSet<>();
    }

    @Override
    public IndexPath visit(BinaryExpression binaryExpression) throws QueryException {
        IndexPath indexPath = new IndexPath();

        // TODO: what about cases where expressions are similar to "where id + 4 > 0" for example. This use case must be handled as well

        if (isValueComparison(binaryExpression)) {
            IdentifierExpression identifierExpression = (IdentifierExpression) (binaryExpression.getLeft() instanceof IdentifierExpression ? binaryExpression.getLeft() : binaryExpression.getRight());

            if (rangeSymbols.contains((Symbol) binaryExpression.getOperator())) {
                boolean valueIsLeftSide = (binaryExpression.getLeft() instanceof ValueExpression<?>);
                ValueExpression<?> valueExpression = (ValueExpression<?>) (binaryExpression.getLeft() instanceof ValueExpression<?> ? binaryExpression.getLeft() : binaryExpression.getRight());

                ValueComparator<?> valueComparator = determineBounds((Symbol) binaryExpression.getOperator(), valueExpression, valueIsLeftSide);

                indexPath.addPath(new DefaultPathNode(identifierExpression.getName(), valueComparator, DefaultPathNode.IndexType.fromBoolean(SchemaSearcher.columnIsIndexed(table, identifierExpression.getName()))));

                return indexPath;
            }
        }

        boolean isAndOperator = binaryExpression.getOperator() instanceof Keyword keyword && keyword == Keyword.And;
        boolean isOrOperator = binaryExpression.getOperator() instanceof Keyword keyword && keyword == Keyword.Or;

        if (isAndOperator || isOrOperator) {
            IndexPath leftPaths = binaryExpression.getLeft().accept(this);
            IndexPath rightPaths = binaryExpression.getRight().accept(this);

            if (isAndOperator) {
                return processAndOperator(leftPaths, rightPaths);
            } else {
                return processOrOperator(leftPaths, rightPaths);
            }
        }

        return indexPath;
    }

    @Override
    public IndexPath visit(NestedExpression nestedExpression) throws QueryException {
        return nestedExpression.getExpression().accept(this);
    }

    private IndexPath processAndOperator(IndexPath leftPaths, IndexPath rightPaths) {
        if (leftPaths.isEmpty() && rightPaths.isEmpty()) {
            return leftPaths;
        }

        for (DefaultPathNode node : Set.of(leftPaths.getNodePaths(), rightPaths.getNodePaths()).stream().flatMap(Collection::stream).toList()) {
            if (canceledColumnPaths.contains(node.getColumn())) {
                return new IndexPath();
            }
        }

        Map<String, List<DefaultPathNode>> nodesGroupedByColumn = new HashMap<>();
        for (DefaultPathNode node : Set.of(leftPaths.getNodePaths(), rightPaths.getNodePaths()).stream().flatMap(Collection::stream).toList()) {
            if (nodesGroupedByColumn.containsKey(node.getColumn())) {
                nodesGroupedByColumn.get(node.getColumn()).add(node);
            } else {
                nodesGroupedByColumn.put(node.getColumn(), new ArrayList<>(List.of(node)));
            }
        }

        IndexPath mergedPaths = new IndexPath();
        for (Map.Entry<String, List<DefaultPathNode>> columnEntry : nodesGroupedByColumn.entrySet()) {
            if (columnEntry.getValue().size() == 1) {
                mergedPaths.addPath(columnEntry.getValue().getFirst());
            } else {
                Optional<DefaultPathNode> possibleMergedIntersection;
                try {
                    possibleMergedIntersection = mergeIntersection(columnEntry.getValue());
                    possibleMergedIntersection.ifPresent(mergedPaths::addPath);
                } catch (QueryException | ParserException e) {
                    canceledColumnPaths.add(columnEntry.getKey());
                    return new IndexPath();
                }
            }
        }

        return mergedPaths;
    }

    private IndexPath processOrOperator(IndexPath leftPaths, IndexPath rightPaths) {
        if (leftPaths.isEmpty() && rightPaths.isEmpty()) {
            return leftPaths;
        }

        IndexPath mergedIndexPath = new IndexPath();
        Set.of(leftPaths.getNodePaths(), rightPaths.getNodePaths()).stream().flatMap(Collection::stream).forEach(mergedIndexPath::addPath);

        return mergedIndexPath;
    }

    private <V> Optional<DefaultPathNode> mergeIntersection(List<DefaultPathNode> nodes) throws QueryException, ParserException {
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

        DefaultPathNode newNode = new DefaultPathNode(nodes.getFirst().getColumn(), initialComparator, DefaultPathNode.IndexType.fromBoolean(SchemaSearcher.columnIsIndexed(table, nodes.getFirst().getColumn())));
        return Optional.of(newNode);
    }

    private static boolean isValueComparison(BinaryExpression binaryExpression) {
        return (binaryExpression.getLeft() instanceof IdentifierExpression && binaryExpression.getRight() instanceof ValueExpression<?>) ||
                (binaryExpression.getRight() instanceof IdentifierExpression && binaryExpression.getLeft() instanceof ValueExpression<?>);
    }

    private static ValueComparator<?> determineBounds(Symbol operator, ValueExpression<?> valueExpression, boolean valueIsLeftSide) throws QueryException {
        return switch (operator) {
            case Eq -> buildEqualityComparator(valueExpression, true);
            case Neq -> buildEqualityComparator(valueExpression, false);
            case Lt, Gt ->
                    buildRangeComparator((ValueExpression<SqlNumberValue>) valueExpression, false, valueIsLeftSide);
            case LtEq, GtEq ->
                    buildRangeComparator((ValueExpression<SqlNumberValue>) valueExpression, true, valueIsLeftSide);
            // TODO: fix throw
            default -> throw new QueryException(null);
        };
    }

    private static NumberRangeComparator buildRangeComparator(ValueExpression<SqlNumberValue> valueExpression, boolean inclusive, boolean valueIsLeftSide) {
        SqlNumberValue leftValue = valueIsLeftSide ? valueExpression.getValue() : null;
        SqlNumberValue rightValue = !valueIsLeftSide ? valueExpression.getValue() : null;

        NumberRangeComparator.BoundaryType leftBoundaryType = valueIsLeftSide ? NumberRangeComparator.BoundaryType.Bounded : NumberRangeComparator.BoundaryType.Unbounded;
        NumberRangeComparator.BoundaryType rightBoundaryType = !valueIsLeftSide ? NumberRangeComparator.BoundaryType.Bounded : NumberRangeComparator.BoundaryType.Unbounded;

        NumberRangeComparator.InclusionType inclusionType = inclusive ? NumberRangeComparator.InclusionType.Included : NumberRangeComparator.InclusionType.Excluded;

        return new NumberRangeComparator(leftValue, rightValue, leftBoundaryType, rightBoundaryType, inclusionType, inclusionType);
    }

    private static EqualityComparator<?> buildEqualityComparator(ValueExpression<?> valueExpression, boolean shouldBeEqual) {
        SqlValue<?> value = valueExpression.getValue();
        if (value instanceof SqlNumberValue numberValue) {
            return new NumberEqualityComparator(numberValue, shouldBeEqual);
        } else if (value instanceof SqlStringValue stringValue) {
            return new StringEqualityComparator(stringValue, shouldBeEqual);
        } else {
            return new BooleanEqualityComparator((SqlBoolValue) value, shouldBeEqual);
        }
    }
}
