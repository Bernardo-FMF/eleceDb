package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.IdentifierToken;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KeywordOrIdentifierTokenProcessor implements ITokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Character.isLowerCase(value) || Character.isUpperCase(value) || Character.isDigit(value) || value == '_';
    }

    @Override
    public Token consume(CharStream stream) {
        Iterable<Character> stringValue = stream.takeWhile(this::matches);

        String possibleKeyword = StreamSupport.stream(stringValue.spliterator(), true)
                .map(Object::toString)
                .collect(Collectors.joining(""));

        Keyword keyword = Keyword.getKeyword(possibleKeyword);
        if (Keyword.None != keyword) {
            return new KeywordToken(keyword);
        }

        return new IdentifierToken(possibleKeyword);
    }
}
