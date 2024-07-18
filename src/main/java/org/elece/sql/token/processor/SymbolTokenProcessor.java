package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.Location;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.OperatorNotClosed;
import org.elece.sql.token.error.UnexpectedCharInOperator;
import org.elece.sql.token.model.SymbolToken;
import org.elece.sql.token.model.type.Symbol;

import java.util.List;
import java.util.Objects;

public class SymbolTokenProcessor implements ITokenProcessor<Character> {
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
                    tokenBuilder.token(new SymbolToken(symbol));
                    break;
                }
            } else if (symbol.getSymbolValue().length == 1) {
                tokenBuilder.token(new SymbolToken(symbol));
                break;
            }
        }

        if (!tokenBuilder.hasToken()) {
            if (currentSymbol == '!') {
                if (Objects.isNull(nextSymbol)) {
                    tokenBuilder.error(new OperatorNotClosed(initialLocation, Symbol.Neq));
                } else {
                    tokenBuilder.error(new UnexpectedCharInOperator(initialLocation, Symbol.Neq, nextSymbol));
                }
            }
        }

        return tokenBuilder.build();
    }
}
