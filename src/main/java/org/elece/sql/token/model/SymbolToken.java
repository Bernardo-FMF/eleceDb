package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Symbol;

public class SymbolToken extends Token {
    private final Symbol symbol;

    public SymbolToken(Symbol symbol) {
        this.symbol = symbol;
    }

    public Symbol getSymbol() {
        return symbol;
    }
}
