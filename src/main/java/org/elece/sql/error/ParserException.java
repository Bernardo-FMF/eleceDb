package org.elece.sql.error;

import org.elece.sql.error.SqlException;
import org.elece.sql.error.type.ISqlError;

public class ParserException extends SqlException {
    public ParserException(ISqlError error) {
        super(error);
    }
}
