package org.elece.db.schema.model;

import java.util.Objects;

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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        Index index = (Index) obj;
        return Objects.equals(name, index.name) && Objects.equals(columnName, index.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columnName);
    }

    @Override
    public String toString() {
        return "Index{" +
                "name='" + name + '\'' +
                ", columnName='" + columnName + '\'' +
                '}';
    }
}
