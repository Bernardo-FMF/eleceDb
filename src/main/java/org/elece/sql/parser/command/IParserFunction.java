package org.elece.sql.parser.command;

import org.elece.sql.error.ParserException;
import org.elece.sql.error.TokenizerException;

@FunctionalInterface
public interface IParserFunction<T> {
    T parse() throws ParserException, TokenizerException;
}
