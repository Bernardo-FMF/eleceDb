package org.elece.exception.btree.type;

import org.elece.exception.DbError;

public class IndexNotFoundError implements DbError {
    @Override
    public String format() {
        return "Failed to find index object";
    }
}