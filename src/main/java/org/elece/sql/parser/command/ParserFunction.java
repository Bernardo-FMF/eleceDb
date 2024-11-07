package org.elece.sql.parser.command;

import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;

@FunctionalInterface
public interface ParserFunction<T> {
    T parse() throws ParserException, TokenizerException;
}
