package org.elece.storage.error.type;

public class InternalStorageError implements StorageError {
    private final String message;

    public InternalStorageError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return format("Internal storage error", message);
    }
}
