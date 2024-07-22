package org.elece.sql.parser.command;

import org.elece.sql.parser.StatementWrapper;
import org.elece.sql.parser.error.SqlException;
import org.elece.sql.token.error.TokenizerException;

public interface IKeywordCommand extends ITdopAlgorithm {
    StatementWrapper parse() throws SqlException, TokenizerException;
}
