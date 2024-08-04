package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.error.type.token.UnexpectedOrUnsupportedChar;

public class WildcardProcessor implements ITokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return true;
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        Character unexpectedChar = stream.next();
        return TokenWrapper.builder().error(new UnexpectedOrUnsupportedChar(stream.getLocation(), unexpectedChar)).build();
    }
}
