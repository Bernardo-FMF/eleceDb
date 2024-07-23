package org.elece.sql.parser.expression.internal;

import java.util.List;

public class SqlType {
    private static final int VARCHAR_MAX_SIZE = 255;
    private final Type type;
    private final Integer usize;
    private final List<SqlConstraint> constraints;

    public SqlType(Type type) {
        this(type, 0, List.of());
    }

    public SqlType(Type type, List<SqlConstraint> constraints) {
        this(type, 0, constraints);
    }

    public SqlType(Type type, Integer usize, List<SqlConstraint> constraints) {
        this.type = type;
        this.usize = usize;
        this.constraints = constraints;
    }

    public static SqlType intType = new SqlType(Type.Int, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    public static SqlType bigIntType = new SqlType(Type.BigInt, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    public static SqlType unsignedIntType = new SqlType(Type.UnsignedInt, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    public static SqlType unsignedBigIntType = new SqlType(Type.UnsignedBigInt, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    public static SqlType boolType = new SqlType(Type.Bool);
    public static SqlType varcharType = new SqlType(Type.Varchar, VARCHAR_MAX_SIZE, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));

    public static SqlType varchar(Integer size) {
        if (size >= VARCHAR_MAX_SIZE) {
            return varcharType;
        }
        int validSize = (size <= 0) ? VARCHAR_MAX_SIZE : size;
        return new SqlType(Type.Varchar, validSize, List.of(SqlConstraint.PrimaryKey, SqlConstraint.Unique));
    }

    public enum Type {
        Int,
        BigInt,
        UnsignedInt,
        UnsignedBigInt,
        Bool,
        Varchar
    }
}
