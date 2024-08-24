package org.elece.sql.db.schema;

import org.elece.sql.db.schema.model.Schema;

public class JsonSchemaManager implements SchemaManager {
    private final Schema schema;

    public JsonSchemaManager() {
        this.schema = null;
    }

    public Schema getSchema() {
        return schema;
    }
}
