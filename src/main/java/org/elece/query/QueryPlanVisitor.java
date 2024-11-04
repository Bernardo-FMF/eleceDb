package org.elece.query;

import org.elece.exception.query.QueryException;
import org.elece.query.path.NodeCollection;
import org.elece.sql.parser.expression.BinaryExpression;
import org.elece.sql.parser.expression.NestedExpression;

public interface QueryPlanVisitor {
    NodeCollection visit(BinaryExpression binaryExpression) throws
                                                            QueryException;

    NodeCollection visit(NestedExpression nestedExpression) throws
                                                            QueryException;
}