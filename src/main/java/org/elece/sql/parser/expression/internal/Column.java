package org.elece.sql.parser.expression.internal;

import java.util.List;

public class Column {
    private final String name;
    private final SqlType sqlType;
    private final List<SqlConstraint> constraints;

    public Column(String name, SqlType sqlType, List<SqlConstraint> constraints) {
        this.name = name;
        this.sqlType = sqlType;
        this.constraints = constraints;
    }

    public String getName() {
        return name;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public List<SqlConstraint> getConstraints() {
        return constraints;
    }
}
