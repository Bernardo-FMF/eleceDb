package org.elece.db.schema.model.builder;

import org.elece.db.schema.model.Column;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;

import java.util.List;

public class ColumnBuilder {
    private String name;
    private SqlType sqlType;
    private List<SqlConstraint> constraints;

    private ColumnBuilder() {
        // private constructor
    }

    public static ColumnBuilder builder() {
        return new ColumnBuilder();
    }

    public ColumnBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public ColumnBuilder setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
        return this;
    }

    public ColumnBuilder setConstraints(List<SqlConstraint> constraints) {
        this.constraints = constraints;
        return this;
    }

    public Column build() {
        return new Column(name, sqlType, constraints);
    }
}