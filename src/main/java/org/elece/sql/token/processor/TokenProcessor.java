package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;

public interface TokenProcessor<T> {
    boolean matches(T value);
    TokenWrapper consume(CharStream stream);
}
