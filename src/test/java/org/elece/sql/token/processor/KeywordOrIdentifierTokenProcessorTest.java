package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.IdentifierToken;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.type.Keyword;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KeywordOrIdentifierTokenProcessorTest {
    @Test
    void test_keyword() {
        KeywordOrIdentifierTokenProcessor keywordOrIdentifierTokenProcessor = new KeywordOrIdentifierTokenProcessor();

        String input = "select";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        TokenWrapper wrapper = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(wrapper.getToken() instanceof KeywordToken);
        KeywordToken keywordToken = (KeywordToken) wrapper.getToken();
        Assertions.assertEquals(Keyword.Select, keywordToken.getKeyword());
    }

    @Test
    void test_identifier() {
        KeywordOrIdentifierTokenProcessor keywordOrIdentifierTokenProcessor = new KeywordOrIdentifierTokenProcessor();

        String input = "name";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        TokenWrapper wrapper = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(wrapper.getToken() instanceof IdentifierToken);
        IdentifierToken identifierToken = (IdentifierToken) wrapper.getToken();
        Assertions.assertEquals("name", identifierToken.getIdentifier());
    }

    @Test
    void test_keywordFollowedByIdentifier() {
        KeywordOrIdentifierTokenProcessor keywordOrIdentifierTokenProcessor = new KeywordOrIdentifierTokenProcessor();

        String input = "select name";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        TokenWrapper tokenWrapper = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(tokenWrapper.getToken() instanceof KeywordToken);
        KeywordToken keywordToken = (KeywordToken) tokenWrapper.getToken();
        Assertions.assertEquals(Keyword.Select, keywordToken.getKeyword());

        charStream.next();

        Assertions.assertTrue(keywordOrIdentifierTokenProcessor.matches(charStream.peek()));
        tokenWrapper = keywordOrIdentifierTokenProcessor.consume(charStream);
        Assertions.assertTrue(tokenWrapper.getToken() instanceof IdentifierToken);
        IdentifierToken identifierToken = (IdentifierToken) tokenWrapper.getToken();
        Assertions.assertEquals("name", identifierToken.getIdentifier());
    }
}