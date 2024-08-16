package org.elece.memory.error.type;

import java.util.Objects;

public interface IBTreeError {
    String format();

    default String format(String cause, String message) {
        StringBuilder errorStringBuilder = new StringBuilder();
        errorStringBuilder.append("BTreeError(");

        if (!Objects.isNull(cause)) {
            errorStringBuilder.append("\n\tcause: ").append(cause);
        }

        if (!Objects.isNull(message)) {
            errorStringBuilder.append("\n\tmessage: ").append(message);
        }

        errorStringBuilder.append("\n)");

        return errorStringBuilder.toString();
    }
}
