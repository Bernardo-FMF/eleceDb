package org.elece.sql.parser;

import org.elece.sql.error.ParserException;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.error.TokenizerException;

public interface ISqlParser {
    Statement parse() throws ParserException, TokenizerException;
}
