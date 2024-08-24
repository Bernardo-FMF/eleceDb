package org.elece.sql.db.schema.model;

import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;

import java.util.List;
import java.util.Objects;

public final class Column {
    private int id;
    private final String name;
    private final SqlType sqlType;
    private final List<SqlConstraint> constraints;

    public Column(String name, SqlType sqlType, List<SqlConstraint> constraints) {
        this.name = name;
        this.sqlType = sqlType;
        this.constraints = constraints;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
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

    @Override
    public int hashCode() {
        return Objects.hash(id, name, sqlType, constraints);
    }
}
