package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.SymbolToken;
import org.elece.sql.token.model.type.Symbol;

import java.util.Objects;

public class EofTokenProcessor implements TokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Objects.isNull(value);
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        return TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build();
    }
}
