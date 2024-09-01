package org.elece.sql.token.processor;

import org.elece.exception.sql.type.token.UnexpectedOrUnsupportedCharError;
import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;

public class WildcardProcessor implements ITokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return true;
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        Character unexpectedChar = stream.next();
        return TokenWrapper.builder().error(new UnexpectedOrUnsupportedCharError(stream.getLocation(), unexpectedChar)).build();
    }
}
