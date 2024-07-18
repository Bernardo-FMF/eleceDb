package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.IdentifierToken;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KeywordOrIdentifierTokenProcessor implements ITokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Character.isLowerCase(value) || Character.isUpperCase(value) || Character.isDigit(value) || value == '_';
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        TokenWrapper.Builder tokenBuilder = TokenWrapper.builder();

        Iterable<Character> stringValue = stream.takeWhile(this::matches);

        String possibleKeyword = StreamSupport.stream(stringValue.spliterator(), true)
                .map(Object::toString)
                .collect(Collectors.joining(""));

        Keyword keyword = Keyword.getKeyword(possibleKeyword);
        if (!Objects.isNull(keyword) && Keyword.None != keyword) {
            tokenBuilder.token(new KeywordToken(keyword));
        } else {
            tokenBuilder.token(new IdentifierToken(possibleKeyword));
        }

        return tokenBuilder.build();
    }
}
