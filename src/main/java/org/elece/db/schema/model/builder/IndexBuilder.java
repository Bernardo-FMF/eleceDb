package org.elece.db.schema.model.builder;

import org.elece.db.schema.model.Index;

public class IndexBuilder {
    private String name;
    private String columnName;

    private IndexBuilder() {
        // private constructor
    }

    public static IndexBuilder builder() {
        return new IndexBuilder();
    }

    public IndexBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public IndexBuilder setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public Index build() {
        return new Index(name, columnName);
    }
}