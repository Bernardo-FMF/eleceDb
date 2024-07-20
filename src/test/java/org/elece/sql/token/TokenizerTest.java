package org.elece.sql.token;

import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.*;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;
import org.elece.sql.token.model.type.Whitespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

class TokenizerTest {
    @Test
    public void test_select() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("SELECT id, name FROM users;");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_selectWhere() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("SELECT id, name FROM users where id > 10;");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Gt)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new NumberToken("10")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_selectOrderBy() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("SELECT id, name FROM users ORDER BY name;");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Order)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.By)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_selectWhereWithAndOr() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("SELECT id, name FROM users WHERE id > 5 AND id < 10 OR name = 'name';");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Select)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Gt)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new NumberToken("5")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.And)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Lt)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new NumberToken("10")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Or)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new StringToken("name")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_createTable() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Create)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Table)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Int)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Primary)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Key)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Varchar)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().token(new NumberToken("255")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_updateTable() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("UPDATE users SET name = 'updatedName' WHERE id = 1;");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Update)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Set)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new StringToken("updatedName")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new NumberToken("1")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_deleteFrom() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("DELETE FROM users WHERE id = 1;");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Delete)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.From)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Where)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eq)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new NumberToken("1")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_insertInto() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("INSERT INTO users (id, name) VALUES (1, 'name');");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();
        TokenWrapper[] expectedTokens = new TokenWrapper[]{
                TokenWrapper.builder().token(new KeywordToken(Keyword.Insert)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Into)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("users")).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().token(new IdentifierToken("id")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new IdentifierToken("name")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new KeywordToken(Keyword.Values)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.LeftParenthesis)).build(),
                TokenWrapper.builder().token(new NumberToken("1")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Comma)).build(),
                TokenWrapper.builder().token(new WhitespaceToken(Whitespace.Space)).build(),
                TokenWrapper.builder().token(new StringToken("name")).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.RightParenthesis)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.SemiColon)).build(),
                TokenWrapper.builder().token(new SymbolToken(Symbol.Eof)).build()
        };

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            Assertions.assertEquals(expectedTokens[idx], current);
            idx++;
        } while (actualTokens.hasNext());
    }

    @Test
    public void test_unsupportedChar() throws TokenizerException {
        Tokenizer tokenizer = new Tokenizer("SELECT ^ FROM users ORDER BY name;");
        Iterator<TokenWrapper> actualTokens = tokenizer.tokenize();

        int idx = 0;
        do {
            TokenWrapper current = actualTokens.next();
            if (idx == 2) {
                Assertions.assertTrue(current.hasError());
            }
            idx++;
        } while (actualTokens.hasNext());
    }
}