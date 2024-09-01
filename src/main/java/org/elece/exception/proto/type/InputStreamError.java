package org.elece.exception.proto.type;

import org.elece.exception.DbError;

public class InputStreamError implements DbError {
    @Override
    public String format() {
        return "Error while reading the input stream";
    }
}
