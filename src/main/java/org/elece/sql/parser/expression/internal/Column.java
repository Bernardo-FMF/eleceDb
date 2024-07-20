package org.elece.sql.parser.expression.internal;

import java.util.List;

public class Column {
    private final String name;
    private final SqlType sqlType;
    private final List<SqlTypeCapability> constraints;

    private Column(String name, SqlType sqlType, List<SqlTypeCapability> constraints) {
        this.name = name;
        this.sqlType = sqlType;
        this.constraints = constraints;
    }

    public static Column column(String name, SqlType sqlType) {
        return new Column(name, sqlType, List.of());
    }

    public static Column primaryKey(String name, SqlType sqlType) {
        return new Column(name, sqlType, List.of(SqlTypeCapability.PrimaryKey));
    }

    public static Column unique(String name, SqlType sqlType) {
        return new Column(name, sqlType, List.of(SqlTypeCapability.Unique));
    }
}
