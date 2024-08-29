package org.elece.sql.db.schema.model.builder;

import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Index;
import org.elece.sql.db.schema.model.Table;

import java.util.List;

public class TableBuilder {
    private int id;
    private String name;
    private List<Column> columns;
    private List<Index> indexes;

    private TableBuilder() {
        // private constructor
    }

    public static TableBuilder builder() {
        return new TableBuilder();
    }

    public TableBuilder setId(int id) {
        this.id = id;
        return this;
    }

    public TableBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public TableBuilder setColumns(List<Column> columns) {
        this.columns = columns;
        return this;
    }

    public TableBuilder setIndexes(List<Index> indexes) {
        this.indexes = indexes;
        return this;
    }

    public Table build() {
        return new Table(id, name, columns, indexes);
    }
}