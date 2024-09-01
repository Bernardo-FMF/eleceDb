package org.elece.sql.parser.command;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;

@FunctionalInterface
public interface IParserFunction<T> {
    T parse() throws ParserException, TokenizerException;
}
