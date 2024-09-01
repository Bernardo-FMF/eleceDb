package org.elece.exception;

public abstract class BaseDbException extends Exception {
    public BaseDbException(String message) {
        super(message);
    }

    public abstract String getFormattedMessage();
}
