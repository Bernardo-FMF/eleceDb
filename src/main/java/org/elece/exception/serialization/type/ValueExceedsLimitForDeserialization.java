package org.elece.exception.serialization.type;

import org.elece.exception.DbError;

public class ValueExceedsLimitForDeserialization implements DbError {
    private final int size;
    private final int maxSize;

    public ValueExceedsLimitForDeserialization(int maxSize, int size) {
        this.maxSize = maxSize;
        this.size = size;
    }

    @Override
    public String format() {
        return String.format("Value exceeds size for deserialization, maximum size is %d bytes but found %d", maxSize, size);
    }
}
