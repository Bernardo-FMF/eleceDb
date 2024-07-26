package org.elece.sql.parser;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.error.TokenizerException;

public interface ISqlParser {
    Statement parse() throws SqlException, TokenizerException;
}
