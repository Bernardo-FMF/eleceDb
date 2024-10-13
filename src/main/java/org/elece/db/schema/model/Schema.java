package org.elece.db.schema.model;

import java.util.List;
import java.util.Objects;

public class Schema {
    private final String dbName;
    private final List<Table> tables;

    public Schema(String dbName, List<Table> tables) {
        this.dbName = dbName;
        this.tables = tables;
    }

    public String getDbName() {
        return dbName;
    }

    public List<Table> getCollections() {
        return tables;
    }

    public void addTable(Table table) {
        tables.add(table);
    }

    public void removeTable(String tableName) {
        tables.removeIf(table -> table.getName().equals(tableName));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        Schema schema = (Schema) obj;
        return Objects.equals(dbName, schema.dbName) && Objects.equals(tables, schema.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tables);
    }
}
