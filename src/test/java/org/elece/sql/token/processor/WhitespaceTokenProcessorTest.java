package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.WhitespaceToken;
import org.elece.sql.token.model.type.Whitespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

class WhitespaceTokenProcessorTest {
    private static final Set<String> CHARACTERS = Set.of(" ", "\t", "\n", "\r\n");

    private static final Map<String, Whitespace> CHARACTER_MAP = Map.of(
            " ", Whitespace.Space,
            "\t", Whitespace.Tab,
            "\n", Whitespace.NewLine,
            "\r\n", Whitespace.NewLine
    );

    @Test
    public void test_allCharacters() {
        WhitespaceTokenProcessor whitespaceTokenProcessor = new WhitespaceTokenProcessor();
        CHARACTERS.forEach(character -> {
            CharStream charStream = new CharStream(character);
            Assertions.assertTrue(whitespaceTokenProcessor.matches(charStream.peek()));
            WhitespaceToken token = whitespaceTokenProcessor.consume(charStream);
            Assertions.assertEquals(CHARACTER_MAP.get(character), token.getWhitespace());
        });
    }
}