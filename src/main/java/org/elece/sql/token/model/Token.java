package org.elece.sql.token.model;

import org.elece.sql.token.model.type.*;

public enum Token {
    Eq(new SymbolToken("=")),
    Neq(new SymbolToken("!=")),
    Lt(new SymbolToken("<")),
    Gt(new SymbolToken(">")),
    LtEq(new SymbolToken("<=")),
    GtEq(new SymbolToken(">=")),
    Mul(new SymbolToken("*")),
    Div(new SymbolToken("/")),
    Plus(new SymbolToken("+")),
    Minus(new SymbolToken("-")),
    LeftParenthesis(new SymbolToken("(")),
    RightParenthesis(new SymbolToken(")")),
    Comma(new SymbolToken(",")),
    Semicolon(new SymbolToken(";")),
    Eof(new SymbolToken("EOF"));

    private final IToken token;

    Token(IToken token) {
        this.token = token;
    }

    public static KeywordToken createKeywordToken(Keyword keyword) {
        return new KeywordToken(keyword);
    }

    public static WhitespaceToken createWhitespaceToken(Whitespace whitespace) {
        return new WhitespaceToken(whitespace);
    }

    public static NumberToken createNumberToken(String number) {
        return new NumberToken(number);
    }

    public static StringToken createStringToken(String string) {
        return new StringToken(string);
    }

    public static IdentifierToken createIdentifierToken(String identifier) {
        return new IdentifierToken(identifier);
    }

    public IToken getToken() {
        return token;
    }
}
