package org.elece.db.schema.model;

import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.sql.parser.expression.internal.SqlType;

import java.util.List;
import java.util.Objects;

public final class Column {
    public static final String CLUSTER_ID = "cluster_id";

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

    public void addConstraint(SqlConstraint constraint) {
        constraints.add(constraint);
    }

    public boolean isUnique() {
        return constraints.contains(SqlConstraint.PrimaryKey) || constraints.contains(SqlConstraint.Unique);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        Column column = (Column) obj;
        return id == column.id && Objects.equals(name, column.name) && Objects.equals(sqlType, column.sqlType) && Objects.equals(constraints, column.constraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, sqlType, constraints);
    }
}
