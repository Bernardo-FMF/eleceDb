package org.elece.sql.db.schema.model;

import java.util.List;

public class Schema {
    private final String dbName;
    private final List<Collection> collections;

    public Schema(String dbName, List<Collection> collections) {
        this.dbName = dbName;
        this.collections = collections;
    }

    public String getDbName() {
        return dbName;
    }

    public List<Collection> getCollections() {
        return collections;
    }
}
