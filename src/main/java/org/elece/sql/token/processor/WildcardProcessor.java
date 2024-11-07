package org.elece.sql.token.processor;

import org.elece.exception.DbError;
import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;

public class WildcardProcessor implements TokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return true;
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        Character unexpectedChar = stream.next();
        return TokenWrapper.builder().setError(DbError.UNEXPECTED_OR_UNSUPPORTED_CHARACTER_ERROR, String.format("Unexpected or unsupported character %c found on (%s, %s)", unexpectedChar, stream.getLocation().getLine(), stream.getLocation().getColumn())).build();
    }
}
