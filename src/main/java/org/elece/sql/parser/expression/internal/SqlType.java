package org.elece.sql.parser.expression.internal;

import java.util.List;

public class SqlType {
    private static final int VARCHAR_MAX_SIZE = 255;

    private final Type type;
    private final Integer size;
    private final List<SqlConstraint> constraints;

    public SqlType(Type type) {
        this(type, 0, List.of());
    }

    public SqlType(Type type, List<SqlConstraint> constraints) {
        this(type, 0, constraints);
    }

    public SqlType(Type type, Integer size, List<SqlConstraint> constraints) {
        this.type = type;
        this.size = size;
        this.constraints = constraints;
    }

    public static SqlType intType = new SqlType(Type.Int, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    public static SqlType boolType = new SqlType(Type.Bool);
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
}
