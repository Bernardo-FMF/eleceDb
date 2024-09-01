package org.elece.exception.proto.type;

import org.elece.exception.DbError;

public class InvalidHeaderSizeError implements DbError {
    private final int actualSize;
    private final int expectedSize;

    public InvalidHeaderSizeError(int actualSize, int expectedSize) {
        this.actualSize = actualSize;
        this.expectedSize = expectedSize;
    }

    @Override
    public String format() {
        return "Size header is not valid, expected %o but read %o bytes";
    }
}
