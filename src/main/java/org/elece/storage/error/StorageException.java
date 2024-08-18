package org.elece.storage.error;

import org.elece.storage.error.type.IStorageError;

public class StorageException extends Exception {
    private final IStorageError error;

    public StorageException(IStorageError error) {
        super(error.format());
        this.error = error;
    }
}
