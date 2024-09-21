package org.elece.exception.btree.type;

import org.elece.exception.DbError;

public class FailedIndexCreationError<K, V> implements DbError {
    private final K key;
    private final V value;

    public FailedIndexCreationError(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String format() {
        return String.format("Failed to insert '%s' into the tree. Value is %s.", key, value);
    }
}
