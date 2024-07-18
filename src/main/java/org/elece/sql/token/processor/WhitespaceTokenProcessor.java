package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.WhitespaceToken;
import org.elece.sql.token.model.type.Whitespace;

import java.util.List;

public class WhitespaceTokenProcessor implements ITokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Whitespace.canMatch(value);
    }

    @Override
    public WhitespaceToken consume(CharStream stream) {
        Character next = stream.next();
        if (Whitespace.CarriageNewLine.getWhitespaceValue()[0] == next) {
            Character nextWhitespace = stream.peek();
            if (Whitespace.CarriageNewLine.getWhitespaceValue()[1] == nextWhitespace) {
                return new WhitespaceToken(Whitespace.NewLine);
            }
        }

        List<Whitespace> whitespaces = Whitespace.matchableWhitespaces(next);
        if (whitespaces.size() == 1) {
            return new WhitespaceToken(whitespaces.get(0));
        }

        return null;
    }
}
