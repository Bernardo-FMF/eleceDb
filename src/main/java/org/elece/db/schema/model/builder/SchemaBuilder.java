package org.elece.db.schema.model.builder;

import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;

import java.util.List;

public class SchemaBuilder {
    private String dbName;
    private List<Table> tables;

    private SchemaBuilder() {
        // private constructor
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder();
    }

    public SchemaBuilder setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public SchemaBuilder setTables(List<Table> tables) {
        this.tables = tables;
        return this;
    }

    public Schema build() {
        return new Schema(dbName, tables);
    }
}