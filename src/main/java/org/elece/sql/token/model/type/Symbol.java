package org.elece.sql.token.model.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public enum Symbol implements IOperator {
    LT_EQ(true, false, '<', '='),
    GT_EQ(true, false, '>', '='),
    NEQ(true, false, '!', '='),
    EQ(true, false, '='),
    LT(true, false, '<'),
    GT(true, false, '>'),
    MUL(true, false, '*'),
    DIV(true, false, '/'),
    PLUS(true, true, '+'),
    MINUS(true, true, '-'),
    LEFT_PARENTHESIS('('),
    RIGHT_PARENTHESIS(')'),
    COMMA(','),
    SEMI_COLON(';'),
    EOF();

    private static final Symbol[] SYMBOL_VALUES = values();
    private final char[] symbolValue;
    private final boolean isUnaryOperator;
    private final boolean isBinaryOperator;

    Symbol(char... symbolValue) {
        this(false, false, symbolValue);
    }

    Symbol() {
        this(false, false, null);
    }

    Symbol(Boolean isBinaryOperator, Boolean isUnaryOperator, char... symbolValue) {
        this.symbolValue = symbolValue;
        this.isBinaryOperator = isBinaryOperator;
        this.isUnaryOperator = isUnaryOperator;
    }

    public static boolean canMatch(Character character) {
        for (Symbol symbol : SYMBOL_VALUES) {
            if (!Objects.isNull(symbol.symbolValue) && symbol.symbolValue[0] == character) {
                return true;
            }
        }
        return false;
    }

    public static List<Symbol> matchableSymbols(Character character) {
        List<Symbol> symbols = new ArrayList<>();
        for (Symbol symbol : SYMBOL_VALUES) {
            if (!Objects.isNull(symbol.symbolValue) && symbol.symbolValue[0] == character) {
                symbols.add(symbol);
            }
        }
        return symbols;
    }

    public char[] getSymbolValue() {
        return symbolValue;
    }

    @Override
    public boolean isBinaryOperator() {
        return isBinaryOperator;
    }

    @Override
    public boolean isUnaryOperator() {
        return isUnaryOperator;
    }
}
