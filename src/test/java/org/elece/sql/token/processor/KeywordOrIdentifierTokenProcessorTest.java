package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.IdentifierToken;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KeywordOrIdentifierTokenProcessorTest {
    @Test
    public void test_keyword() {
        KeywordOrIdentifierTokenProcessor keywordOrIdentifierTokenProcessor = new KeywordOrIdentifierTokenProcessor();

        String input = "select";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        Token token = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(token instanceof KeywordToken);
        KeywordToken keywordToken = (KeywordToken) token;
        Assertions.assertEquals(Keyword.Select, keywordToken.getKeyword());
    }

    @Test
    public void test_identifier() {
        KeywordOrIdentifierTokenProcessor keywordOrIdentifierTokenProcessor = new KeywordOrIdentifierTokenProcessor();

        String input = "name";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        Token token = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(token instanceof IdentifierToken);
        IdentifierToken identifierToken = (IdentifierToken) token;
        Assertions.assertEquals("name", identifierToken.getIdentifier());
    }

    @Test
    public void test_keywordFollowedByIdentifier() {
        KeywordOrIdentifierTokenProcessor keywordOrIdentifierTokenProcessor = new KeywordOrIdentifierTokenProcessor();

        String input = "select name";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        Token token = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(token instanceof KeywordToken);
        KeywordToken keywordToken = (KeywordToken) token;
        Assertions.assertEquals(Keyword.Select, keywordToken.getKeyword());

        charStream.next();

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        token = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(token instanceof IdentifierToken);
        IdentifierToken identifierToken = (IdentifierToken) token;
        Assertions.assertEquals("name", identifierToken.getIdentifier());
    }
}