package org.elece.sql.parser.command;

import org.elece.sql.parser.error.ParserException;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.error.TokenizerException;

public interface IKeywordCommand extends ITdopAlgorithm {
    Statement parse() throws ParserException, TokenizerException;
}
