package org.elece.db.schema.model;

import java.util.List;

public class Table {
    private int id;
    private final String name;
    private final List<Column> columns;
    private final List<Index> indexes;

    public Table(String name, List<Column> columns, List<Index> indexes) {
        this.name = name;
        this.columns = columns;
        this.indexes = indexes;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void addIndex(Index index) {
        indexes.add(index);
    }
}
