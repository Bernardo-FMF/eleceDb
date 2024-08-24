package org.elece.sql.db.schema.model;

import java.util.List;

public class Collection {
    private final int id;
    private final String name;
    private final List<Column> columns;
    private final List<Index> indexes;

    public Collection(int id, String name, List<Column> columns, List<Index> indexes) {
        this.id = id;
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
}
