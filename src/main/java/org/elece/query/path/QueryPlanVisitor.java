package org.elece.query.path;

import org.elece.exception.QueryException;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.NestedExpression;

public interface QueryPlanVisitor {
    NodeCollection visit(BinaryExpression binaryExpression) throws QueryException;

    NodeCollection visit(NestedExpression nestedExpression) throws QueryException;
}