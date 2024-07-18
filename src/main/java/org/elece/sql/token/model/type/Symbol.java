package org.elece.sql.token.model.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public enum Symbol {
    LtEq('<', '='),
    GtEq('>', '='),
    Neq('!', '='),
    Eq('='),
    Lt('<'),
    Gt('>'),
    Mul('*'),
    Div('/'),
    Plus('+'),
    Minus('-'),
    LeftParenthesis('('),
    RightParenthesis(')'),
    Comma(','),
    SemiColon(';'),
    Eof();

    public static final Symbol[] VALUES = values();
    private final char[] symbolValue;

    Symbol(char... symbolValue) {
        this.symbolValue = symbolValue;
    }

    Symbol() {
        this.symbolValue = null;
    }

    public static boolean canMatch(Character character) {
        for (Symbol symbol : VALUES) {
            if (!Objects.isNull(symbol.symbolValue) && symbol.symbolValue[0] == character) {
                return true;
            }
        }
        return false;
    }

    public static List<Symbol> matchableSymbols(Character character) {
        List<Symbol> symbols = new ArrayList<>();
        for (Symbol symbol : VALUES) {
            if (!Objects.isNull(symbol.symbolValue) && symbol.symbolValue[0] == character) {
                symbols.add(symbol);
            }
        }
        return symbols;
    }

    public char[] getSymbolValue() {
        return symbolValue;
    }
}
