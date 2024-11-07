package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.WhitespaceToken;
import org.elece.sql.token.model.type.Whitespace;

import java.util.List;

public class WhitespaceTokenProcessor implements TokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Whitespace.canMatch(value);
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        TokenWrapper.Builder tokenBuilder = TokenWrapper.builder();

        Character next = stream.next();

        if (Whitespace.CarriageNewLine.getWhitespaceValue()[0] == next) {
            Character nextWhitespace = stream.peek();
            if (Whitespace.CarriageNewLine.getWhitespaceValue()[1] == nextWhitespace) {
                tokenBuilder.setToken(new WhitespaceToken(Whitespace.NewLine));
            }
        }

        if (!tokenBuilder.hasToken()) {
            List<Whitespace> whitespaces = Whitespace.matchableWhitespaces(next);
            if (whitespaces.size() == 1) {
                tokenBuilder.setToken(new WhitespaceToken(whitespaces.get(0)));
            }
        }

        return tokenBuilder.build();
    }
}
