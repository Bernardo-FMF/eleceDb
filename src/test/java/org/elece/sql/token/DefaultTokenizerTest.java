package org.elece.sql.token;

import org.elece.sql.token.model.*;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;
import org.elece.sql.token.model.type.Whitespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultTokenizerTest {
    @Test
    void test_select() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.SELECT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.COMMA)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.FROM)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_selectWhere() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users where id > 10;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.SELECT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.COMMA)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.FROM)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.WHERE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.GT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new NumberToken("10")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_selectOrderBy() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users ORDER BY name;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.SELECT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.COMMA)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.FROM)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.ORDER)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.BY)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_selectWhereWithAndOr() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users WHERE id > 5 AND id < 10 OR name = 'name';").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.SELECT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.COMMA)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.FROM)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.WHERE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.GT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new NumberToken("5")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.AND)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new NumberToken("10")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.OR)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EQ)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new StringToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_createTable() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.CREATE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.TABLE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LEFT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.INT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.PRIMARY)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.KEY)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.COMMA)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.VARCHAR)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LEFT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new NumberToken("255")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RIGHT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RIGHT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_updateTable() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("UPDATE users SET name = 'updatedName' WHERE id = 1;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.UPDATE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.SET)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EQ)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new StringToken("updatedName")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.WHERE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EQ)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new NumberToken("1")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_deleteFrom() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("DELETE FROM users WHERE id = 1;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.DELETE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.FROM)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.WHERE)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EQ)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new NumberToken("1")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_insertInto() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("INSERT INTO users (id, name) VALUES (1, 'name');").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.INSERT)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.INTO)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LEFT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.COMMA)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RIGHT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.VALUES)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LEFT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new NumberToken("1")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.COMMA)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.SPACE)).build(),
                TokenWrapper.builder().setToken(new StringToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RIGHT_PARENTHESIS)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SEMI_COLON)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.EOF)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    void test_unsupportedChar() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT ^ FROM users ORDER BY name;").tokenize();

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            if (idx == 2) {
                Assertions.assertTrue(current.hasError());
            }
            idx++;
        } while (tokenizer.hasNext());
    }
}