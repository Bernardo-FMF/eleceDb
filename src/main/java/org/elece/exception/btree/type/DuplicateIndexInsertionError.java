package org.elece.exception.btree.type;

import org.elece.exception.DbError;

public class DuplicateIndexInsertionError<K> implements DbError {
    private final K key;

    public DuplicateIndexInsertionError(K key) {
        this.key = key;
    }

    @Override
    public String format() {
        return String.format("Index '%s' already exists", key);
    }
}
