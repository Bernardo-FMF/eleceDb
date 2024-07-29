package org.elece.sql.parser.command;

import org.elece.sql.parser.error.ParserException;
import org.elece.sql.token.error.TokenizerException;

@FunctionalInterface
public interface IParserFunction<T> {
    T parse() throws ParserException, TokenizerException;
}
