package org.elece.sql.parser.command;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.token.error.TokenizerException;

@FunctionalInterface
public interface IParserFunction<T> {
    T parse() throws SqlException, TokenizerException;
}
