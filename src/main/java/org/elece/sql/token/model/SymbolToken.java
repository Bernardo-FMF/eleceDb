package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Symbol;

import java.util.Objects;

public class SymbolToken extends Token {
    private final Symbol symbol;

    public SymbolToken(Symbol symbol) {
        super(TokenType.SYMBOL_TOKEN);
        this.symbol = symbol;
    }

    public Symbol getSymbol() {
        return symbol;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        SymbolToken that = (SymbolToken) obj;
        return getSymbol() == that.getSymbol();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSymbol());
    }
}
