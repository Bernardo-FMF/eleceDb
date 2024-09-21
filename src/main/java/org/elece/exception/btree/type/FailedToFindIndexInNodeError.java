package org.elece.exception.btree.type;

import org.elece.exception.DbError;

public class FailedToFindIndexInNodeError implements DbError {
    @Override
    public String format() {
        return "Failed to find index in node to insert key";
    }
}
