package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class IndexNotUniqueError implements DbError {
    private final String indexName;

    public IndexNotUniqueError(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String format() {
        return String.format("Index %s is not unique", indexName);
    }
}
