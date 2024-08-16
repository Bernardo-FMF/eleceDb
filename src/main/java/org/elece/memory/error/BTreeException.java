package org.elece.memory.error;

import org.elece.memory.error.type.IBTreeError;

public class BTreeException extends Exception {
    private final IBTreeError error;

    public BTreeException(IBTreeError error) {
        super(error.format());
        this.error = error;
    }
}
