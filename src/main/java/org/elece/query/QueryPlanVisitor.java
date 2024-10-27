package org.elece.query;

import org.elece.exception.query.QueryException;
import org.elece.query.path.IndexPath;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.NestedExpression;

public interface QueryPlanVisitor {
    IndexPath visit(BinaryExpression binaryExpression) throws QueryException;

    IndexPath visit(NestedExpression nestedExpression) throws QueryException;
}