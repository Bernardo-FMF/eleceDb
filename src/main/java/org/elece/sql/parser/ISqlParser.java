package org.elece.sql.parser;

import org.elece.sql.parser.error.ParserException;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.error.TokenizerException;

public interface ISqlParser {
    Statement parse() throws ParserException, TokenizerException;
}
