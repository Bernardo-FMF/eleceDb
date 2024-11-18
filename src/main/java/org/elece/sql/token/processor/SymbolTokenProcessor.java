package org.elece.sql.token.processor;

import org.elece.exception.DbError;
import org.elece.sql.token.CharStream;
import org.elece.sql.token.Location;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.SymbolToken;
import org.elece.sql.token.model.type.Symbol;

import java.util.List;
import java.util.Objects;

public class SymbolTokenProcessor implements TokenProcessor<Character> {
    @Override
    public boolean matches(Character value) {
        return Symbol.canMatch(value);
    }

    @Override
    public TokenWrapper consume(CharStream stream) {
        TokenWrapper.Builder tokenBuilder = TokenWrapper.builder();

        Character currentSymbol = stream.next();
        Location initialLocation = stream.getLocation();

        List<Symbol> symbols = Symbol.matchableSymbols(currentSymbol);
        Character nextSymbol = stream.peek();

        for (Symbol symbol : symbols) {
            if (symbol.getSymbolValue().length > 1) {
                if (!Objects.isNull(nextSymbol) && symbol.getSymbolValue()[1] == nextSymbol) {
                    stream.next();
                    tokenBuilder.setToken(new SymbolToken(symbol));
                    break;
                }
            } else if (symbol.getSymbolValue().length == 1) {
                tokenBuilder.setToken(new SymbolToken(symbol));
                break;
            }
        }

        if (!tokenBuilder.hasToken() && currentSymbol == '!') {
                if (Objects.isNull(nextSymbol)) {
                    tokenBuilder.setError(DbError.OPERATOR_NOT_CLOSED_ERROR, String.format("Operator not closed, expected symbol %s on (%s, %s)", String.valueOf(Symbol.NEQ.getSymbolValue()), initialLocation.getLine(), initialLocation.getColumn()));
                } else {
                    tokenBuilder.setError(DbError.UNEXPECTED_CHARACTER_IN_OPERATOR_ERROR, String.format("Expected symbol %s, but found %c on (%s, %s)", String.valueOf(Symbol.NEQ.getSymbolValue()), nextSymbol, initialLocation.getLine(), initialLocation.getColumn()));
                }
            }


        return tokenBuilder.build();
    }
}
