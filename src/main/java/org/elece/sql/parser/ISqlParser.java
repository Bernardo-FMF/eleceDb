package org.elece.sql.parser;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.token.error.TokenizerException;

public interface ISqlParser {
    StatementWrapper parseToken() throws SqlException, TokenizerException;
}
