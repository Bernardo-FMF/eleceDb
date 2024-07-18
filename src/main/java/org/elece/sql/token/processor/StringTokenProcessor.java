package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.StringToken;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class StringTokenProcessor implements ITokenProcessor<Character> {
    private static final Set<Character> STRING_STARTS = Set.of('"', '\'');

    @Override
    public boolean matches(Character value) {
        return STRING_STARTS.contains(value);
    }

    @Override
    public StringToken consume(CharStream stream) {
        Character quotationMark = stream.next();

        Iterable<Character> stringValue = stream.takeWhile(character -> character != quotationMark);

        //TODO validate that the quotation mark is the same
        StringToken stringToken = new StringToken(StreamSupport.stream(stringValue.spliterator(), true)
                .map(Object::toString)
                .collect(Collectors.joining("")));

        Character closingQuotationMark = stream.next();
        return stringToken;
    }
}
