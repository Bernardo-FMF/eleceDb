package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

import java.util.List;

public class SelectStatement extends Statement {
    private List<Expression> columns;
    private final String from;
    private Expression where;
    private List<Expression> orderBy;

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

    public void setColumns(List<Expression> columns) {
        this.columns = columns;
    }

    public void setWhere(Expression where) {
        this.where = where;
    }

    public void setOrderBy(List<Expression> orderBy) {
        this.orderBy = orderBy;
    }
}
