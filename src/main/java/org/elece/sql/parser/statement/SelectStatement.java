package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

import java.util.List;

public class SelectStatement extends Statement {
    private final List<Expression> columns;
    private final String from;
    private final Expression where;
    private final List<Expression> orderBy;

    public SelectStatement(List<Expression> columns, String from, Expression where, List<Expression> orderBy) {
        super(StatementType.Select);
        this.columns = columns;
        this.from = from;
        this.where = where;
        this.orderBy = orderBy;
    }

    public List<Expression> getColumns() {
        return columns;
    }

    public String getFrom() {
        return from;
    }

    public Expression getWhere() {
        return where;
    }

    public List<Expression> getOrderBy() {
        return orderBy;
    }
}
