package org.elece.exception.btree.type;

import org.elece.exception.DbError;

public class NodeMismatchError implements DbError {
    private final String message;

    public NodeMismatchError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return message;
    }
}