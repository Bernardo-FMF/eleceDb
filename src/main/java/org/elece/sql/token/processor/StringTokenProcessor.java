package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.Location;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.StringNotClosed;
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
    public TokenWrapper consume(CharStream stream) {
        TokenWrapper.Builder tokenBuilder = TokenWrapper.builder();

        Character quotationMark = stream.next();
        Location initialLocation = stream.getLocation();

        Iterable<Character> stringValue = stream.takeWhile(character -> character != quotationMark);

        StringToken stringToken = new StringToken(StreamSupport.stream(stringValue.spliterator(), true)
                .map(Object::toString)
                .collect(Collectors.joining("")));

        Character closingQuotationMark = stream.next();
        if (quotationMark == closingQuotationMark) {
            tokenBuilder.token(stringToken);
        } else {
            tokenBuilder.error(new StringNotClosed(initialLocation));
        }

        return tokenBuilder.build();
    }
}
