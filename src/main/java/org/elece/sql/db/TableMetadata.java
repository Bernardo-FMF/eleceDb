package org.elece.sql.db;

import java.util.List;

public final class TableMetadata {
    private final String name;
    private final Schema schema;
    private final List<IndexMetadata> indexMetadata;

    public TableMetadata(String name, Schema schema, List<IndexMetadata> indexMetadata) {
        this.name = name;
        this.schema = schema;
        this.indexMetadata = indexMetadata;
    }

    public String getName() {
        return name;
    }

    public Schema getSchema() {
        return schema;
    }

    public List<IndexMetadata> getIndexMetadata() {
        return indexMetadata;
    }
}
