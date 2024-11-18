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

    public static SqlType intType = new SqlType(Type.INT, Integer.BYTES, List.of(SqlConstraint.PRIMARY_KEY, SqlConstraint.UNIQUE));
    public static SqlType boolType = new SqlType(Type.BOOL, 1, List.of());
    public static SqlType varcharType = new SqlType(Type.VARCHAR, VARCHAR_MAX_SIZE, List.of(SqlConstraint.PRIMARY_KEY, SqlConstraint.UNIQUE));

    public static SqlType varchar(Integer size) {
        if (size >= VARCHAR_MAX_SIZE) {
            return varcharType;
        }
        int validSize = (size <= 0) ? VARCHAR_MAX_SIZE : size;
        return new SqlType(Type.VARCHAR, validSize, List.of(SqlConstraint.PRIMARY_KEY, SqlConstraint.UNIQUE));
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
        INT,
        BOOL,
        VARCHAR
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

    @Override
    public String toString() {
        return "SqlType{" +
                "type=" + type +
                ", size=" + size +
                ", constraints=" + constraints +
                '}';
    }
}
