package org.elece.sql.db;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class TableMetadata {
    private final int pageNumber;
    private final String name;
    private final Schema schema;
    private final List<IndexMetadata> indexMetadata;
    private final AtomicLong rowId;

    public TableMetadata(int pageNumber, String name, Schema schema, List<IndexMetadata> indexMetadata) {
        this.pageNumber = pageNumber;
        this.name = name;
        this.schema = schema;
        this.indexMetadata = indexMetadata;
        this.rowId = new AtomicLong(0);
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

    public synchronized Long getNextRowId() {
        return rowId.incrementAndGet();
    }
}
