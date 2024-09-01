package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;
import org.elece.sql.token.model.type.Symbol;

public class UnhandledArithmeticOperatorError implements DbError {
    private final Symbol symbol;

    public UnhandledArithmeticOperatorError(Symbol symbol) {
        this.symbol = symbol;
    }

    @Override
    public String format() {
        return String.format("Arithmetic operator %s is unhandled", new String(symbol.getSymbolValue()));
    }
}
