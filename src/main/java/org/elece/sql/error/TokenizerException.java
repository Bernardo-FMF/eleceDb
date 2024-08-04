package org.elece.sql.error;

import org.elece.sql.error.SqlException;
import org.elece.sql.error.type.ISqlError;

public class TokenizerException extends SqlException {
    public TokenizerException(ISqlError error) {
        super(error);
    }
}
