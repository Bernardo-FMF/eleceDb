package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Symbol;

import java.util.Objects;

public class SymbolToken extends Token {
    private final Symbol symbol;

    public SymbolToken(Symbol symbol) {
        super(TokenType.SymbolToken);
        this.symbol = symbol;
    }

    public Symbol getSymbol() {
        return symbol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SymbolToken that = (SymbolToken) o;
        return getSymbol() == that.getSymbol();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSymbol());
    }
}
