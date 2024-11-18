package org.elece.query.path;

import org.elece.exception.ParserException;
import org.elece.sql.ExpressionUtils;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.internal.SqlBoolValue;
import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ComplexPathNode {
    private final Expression expression;
    private final Set<String> identifiers;

    public ComplexPathNode(Expression expression) {
        this.expression = expression;
        this.identifiers = ExpressionUtils.getIdentifierExpressions(expression).stream()
                .map(IdentifierExpression::getName)
                .collect(Collectors.toSet());
    }

    public Set<String> getIdentifiers() {
        return identifiers;
    }

    public boolean resolve(Map<String, SqlValue<?>> values) throws ParserException {
        SqlBoolValue sqlValue = (SqlBoolValue) ExpressionUtils.resolveExpression(values, expression);
        return sqlValue.getValue();
    }

    @Override
    public String toString() {
        return "ComplexPathNode{" +
                "expression=" + expression +
                ", identifiers=" + identifiers +
                '}';
    }
}
