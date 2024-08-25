package org.elece.sql.db.schema.model;

import java.util.List;

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
}
