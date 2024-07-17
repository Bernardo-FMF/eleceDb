package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.Token;

public interface ITokenProcessor<T> {
    boolean matches(T value);

    Token consume(CharStream stream);
}
