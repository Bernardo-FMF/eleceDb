package org.elece.db.schema.model;

import java.util.List;
import java.util.Objects;

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

    public int getRowSize() {
        return columns.stream()
                .mapToInt(column -> column.getSqlType().getSize())
                .sum();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        Table table = (Table) obj;
        return id == table.id && Objects.equals(name, table.name) && Objects.equals(columns, table.columns) && Objects.equals(indexes, table.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, columns, indexes);
    }
}
