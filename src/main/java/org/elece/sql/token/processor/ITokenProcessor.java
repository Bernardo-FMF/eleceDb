package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;

public interface ITokenProcessor<T> {
    boolean matches(T value);
    TokenWrapper consume(CharStream stream);
}
