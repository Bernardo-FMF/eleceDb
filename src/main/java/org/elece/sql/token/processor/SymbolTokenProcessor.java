package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
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
    public SymbolToken consume(CharStream stream) {
        List<Symbol> symbols = Symbol.matchableSymbols(stream.next());
        for (Symbol symbol : symbols) {
            if (symbol.getSymbolValue().length > 1) {
                Character nextSymbol = stream.peek();
                if (!Objects.isNull(nextSymbol) && symbol.getSymbolValue()[1] == nextSymbol) {
                    return new SymbolToken(symbol);
                }
            } else if (symbol.getSymbolValue().length == 1) {
                return new SymbolToken(symbol);
            }
        }

        return new SymbolToken(Symbol.None);
    }
}
