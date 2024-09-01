package org.elece.sql.parser.command;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.sql.parser.statement.Statement;

public interface IKeywordCommand extends ITdopAlgorithm {
    Statement parse() throws ParserException, TokenizerException;
}
