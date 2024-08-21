package org.elece.storage.error;

import org.elece.storage.error.type.StorageError;

public class StorageException extends Exception {
    private final StorageError error;

    public StorageException(StorageError error) {
        super(error.format());
        this.error = error;
    }
}
