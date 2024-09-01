package org.elece.sql.parser;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.sql.parser.statement.Statement;

public interface ISqlParser {
    Statement parse() throws ParserException, TokenizerException;
}
