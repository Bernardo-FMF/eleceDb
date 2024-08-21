package org.elece.memory.error;

import org.elece.memory.error.type.BTreeError;

public class BTreeException extends Exception {
    private final BTreeError error;

    public BTreeException(BTreeError error) {
        super(error.format());
        this.error = error;
    }
}
