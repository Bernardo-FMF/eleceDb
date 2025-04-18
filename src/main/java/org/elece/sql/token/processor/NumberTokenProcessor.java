package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.NumberToken;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Only supports integer values.
 */
public class NumberTokenProcessor implements TokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Character.isDigit(value);
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        TokenWrapper.Builder tokenBuilder = TokenWrapper.builder();

        Iterable<Character> numberValue = stream.takeWhile(Character::isDigit);

        tokenBuilder.setToken(new NumberToken(StreamSupport.stream(numberValue.spliterator(), true)
                .map(Object::toString)
                .collect(Collectors.joining(""))));

        return tokenBuilder.build();
    }
}
