package org.elece.sql.token;

import org.elece.sql.token.model.*;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;
import org.elece.sql.token.model.type.Whitespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultTokenizerTest {
    @Test
    public void test_select() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_selectWhere() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users where id > 10;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Gt)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new NumberToken("10")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_selectOrderBy() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users ORDER BY name;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Order)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.By)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_selectWhereWithAndOr() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("SELECT id, name FROM users WHERE id > 5 AND id < 10 OR name = 'name';").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Gt)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new NumberToken("5")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.And)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Lt)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new NumberToken("10")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Or)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new StringToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_createTable() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Create)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Table)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Int)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Primary)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Key)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Varchar)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().setToken(new NumberToken("255")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_updateTable() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("UPDATE users SET name = 'updatedName' WHERE id = 1;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Update)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Set)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new StringToken("updatedName")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new NumberToken("1")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_deleteFrom() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("DELETE FROM users WHERE id = 1;").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Delete)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new NumberToken("1")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_insertInto() {
        PeekableIterator<TokenWrapper> tokenizer = new DefaultTokenizer("INSERT INTO users (id, name) VALUES (1, 'name');").tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Insert)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Into)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("users")).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("id")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new IdentifierToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new KeywordToken(Keyword.Values)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().setToken(new NumberToken("1")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().setToken(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().setToken(new StringToken("name")).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().setToken(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = tokenizer.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (tokenizer.hasNext());
    }

    @Test
    public void test_unsupportedChar() {
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