package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class IndexAlreadyExistsError implements DbError {
    private final String indexName;

    public IndexAlreadyExistsError(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String format() {
        return String.format("Index %s already exists", indexName);
    }
}
