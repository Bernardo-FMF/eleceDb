package org.elece.db.schema.model;

public class Index {
    private final String name;
    private final String columnName;

    public Index(String name, String columnName) {
        this.name = name;
        this.columnName = columnName;
    }

    public String getName() {
        return name;
    }

    public String getColumnName() {
        return columnName;
    }
}
