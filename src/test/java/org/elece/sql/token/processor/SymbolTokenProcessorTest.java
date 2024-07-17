package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.SymbolToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class SymbolTokenProcessorTest {
    private static final Set<String> CHARACTERS = Set.of("=", "<", ">", "*", "/", "-", "+", "(", ")", ",", ";", "!=", "<=", ">=");

    @Test
    public void test_allCharacters() {
        SymbolTokenProcessor symbolTokenProcessor = new SymbolTokenProcessor();
        CHARACTERS.forEach(character -> {
            CharStream charStream = new CharStream(character);
            Assertions.assertTrue(symbolTokenProcessor.matches(character.charAt(0)));
            SymbolToken token = symbolTokenProcessor.consume(charStream);
            Assertions.assertArrayEquals(character.toCharArray(), token.getSymbol().getSymbolValue());
        });
    }
}