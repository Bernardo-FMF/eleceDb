package org.elece.sql.token.processor;

import org.elece.exception.DbError;
import org.elece.sql.token.CharStream;
import org.elece.sql.token.Location;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.StringToken;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class StringTokenProcessor implements TokenProcessor<Character> {
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

        Iterable<Character> stringValue = stream.takeWhile(character -> !Objects.equals(character, quotationMark));

        StringToken stringToken = new StringToken(StreamSupport.stream(stringValue.spliterator(), true)
                .map(Object::toString)
                .collect(Collectors.joining("")));

        Character closingQuotationMark = stream.next();
        if (Objects.equals(quotationMark, closingQuotationMark)) {
            tokenBuilder.setToken(stringToken);
        } else {
            tokenBuilder.setError(DbError.STRING_NOT_CLOSED_ERROR, String.format("String is not closed on (%s, %s)", initialLocation.getLine(), initialLocation.getColumn()));
        }

        return tokenBuilder.build();
    }
}
