package org.elece.sql.db.schema.model;

import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;

import java.util.List;
import java.util.Objects;

public final class Column {
    private final int id;
    private final String name;
    private final SqlType sqlType;
    private final List<SqlConstraint> constraints;

    public Column(int id, String name, SqlType sqlType, List<SqlConstraint> constraints) {
        this.id = id;
        this.name = name;
        this.sqlType = sqlType;
        this.constraints = constraints;
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }

    public SqlType sqlType() {
        return sqlType;
    }

    public List<SqlConstraint> constraints() {
        return constraints;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, name, sqlType, constraints);
    }
}
