package org.elece.exception.serialization.type;

import org.elece.exception.DbError;

public class ValueExceedsLimitForSerialization implements DbError {
    private final int size;
    private final int maxSize;

    public ValueExceedsLimitForSerialization(int maxSize, int size) {
        this.maxSize = maxSize;
        this.size = size;
    }

    @Override
    public String format() {
        return String.format("Value exceeds size for serialization, maximum size is %d bytes but found %d", maxSize, size);
    }
}