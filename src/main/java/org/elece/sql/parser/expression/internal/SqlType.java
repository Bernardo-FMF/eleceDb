package org.elece.sql.parser.expression.internal;

import java.util.List;
import java.util.Objects;

public class SqlType {
    private static final int VARCHAR_MAX_SIZE = 255;

    private final Type type;
    private final Integer size;
    private final List<SqlConstraint> constraints;

    public SqlType(Type type, Integer size, List<SqlConstraint> constraints) {
        this.type = type;
        this.size = size;
        this.constraints = constraints;
    }

    public static SqlType intType = new SqlType(Type.Int, Integer.BYTES, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    public static SqlType boolType = new SqlType(Type.Bool, 1, List.of());
    public static SqlType varcharType = new SqlType(Type.Varchar, VARCHAR_MAX_SIZE, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));

    public static SqlType varchar(Integer size) {
        if (size >= VARCHAR_MAX_SIZE) {
            return varcharType;
        }
        int validSize = (size <= 0) ? VARCHAR_MAX_SIZE : size;
        return new SqlType(Type.Varchar, validSize, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    }

    public Type getType() {
        return type;
    }

    public Integer getSize() {
        return size;
    }

    public List<SqlConstraint> getConstraints() {
        return constraints;
    }

    public enum Type {
        Int,
        Bool,
        Varchar
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        SqlType sqlType = (SqlType) obj;
        return type == sqlType.type && Objects.equals(size, sqlType.size) && Objects.equals(constraints, sqlType.constraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, size, constraints);
    }
}
