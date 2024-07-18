package org.elece.sql.token.model.type;

import java.util.ArrayList;
import java.util.List;

public enum Whitespace {
    CarriageNewLine('\r', '\n'),
    Space(' '),
    Tab('\t'),
    NewLine('\n');

    private final char[] whitespaceValue;

    Whitespace(char... whitespaceValue) {
        this.whitespaceValue = whitespaceValue;
    }

    public static final Whitespace[] VALUES = values();

    public static boolean canMatch(Character character) {
        for (Whitespace whitespace : VALUES) {
            if (whitespace.whitespaceValue[0] == character) {
                return true;
            }
        }
        return false;
    }

    public static List<Whitespace> matchableWhitespaces(Character character) {
        List<Whitespace> whitespaces = new ArrayList<>();
        for (Whitespace whitespace : VALUES) {
            if (whitespace.whitespaceValue[0] == character) {
                whitespaces.add(whitespace);
            }
        }
        return whitespaces;
    }

    public char[] getWhitespaceValue() {
        return whitespaceValue;
    }
}