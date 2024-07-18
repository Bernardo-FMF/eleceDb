package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.NumberToken;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Only supports integer values. Float values can be parsed by identifying that we have two NumberToken instances between a comma SymbolToken.
 */
public class NumberTokenProcessor implements ITokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Character.isDigit(value);
    }

    @Override
    public NumberToken consume(CharStream stream) {
        Iterable<Character> numberValue = stream.takeWhile(Character::isDigit);

        return new NumberToken(StreamSupport.stream(numberValue.spliterator(), true)
                .map(Object::toString)
                .collect(Collectors.joining("")));
    }
}
