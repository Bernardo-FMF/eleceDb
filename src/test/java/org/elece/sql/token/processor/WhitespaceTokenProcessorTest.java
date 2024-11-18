package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.WhitespaceToken;
import org.elece.sql.token.model.type.Whitespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

class WhitespaceTokenProcessorTest {
    private static final Set<String> CHARACTERS = Set.of(" ", "\t", "\n", "\r\n");

    private static final Map<String, Whitespace> CHARACTER_MAP = Map.of(
            " ", Whitespace.SPACE,
            "\t", Whitespace.TAB,
            "\n", Whitespace.NEW_LINE,
            "\r\n", Whitespace.NEW_LINE
    );

    @Test
    void test_allCharacters() {
        WhitespaceTokenProcessor whitespaceTokenProcessor = new WhitespaceTokenProcessor();
        CHARACTERS.forEach(character -> {
            CharStream charStream = new CharStream(character);
            Assertions.assertTrue(whitespaceTokenProcessor.matches(charStream.peek()));
            TokenWrapper wrapper = whitespaceTokenProcessor.consume(charStream);
            Assertions.assertTrue(wrapper.getToken() instanceof WhitespaceToken);
            WhitespaceToken whitespaceToken = (WhitespaceToken) wrapper.getToken();
            Assertions.assertEquals(CHARACTER_MAP.get(character), whitespaceToken.getWhitespace());
        });
    }
}