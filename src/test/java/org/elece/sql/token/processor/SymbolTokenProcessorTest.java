package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.SymbolToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class SymbolTokenProcessorTest {
    private static final Set<String> CHARACTERS = Set.of("=", "<", ">", "*", "/", "-", "+", "(", ")", ",", ";", "!=", "<=", ">=");

    @Test
    void test_allCharacters() {
        SymbolTokenProcessor symbolTokenProcessor = new SymbolTokenProcessor();
        CHARACTERS.forEach(character -> {
            CharStream charStream = new CharStream(character);
            Assertions.assertTrue(symbolTokenProcessor.matches(charStream.peek()));
            TokenWrapper wrapper = symbolTokenProcessor.consume(charStream);
            Assertions.assertTrue(wrapper.getToken() instanceof SymbolToken);
            SymbolToken symbolToken = (SymbolToken) wrapper.getToken();
            Assertions.assertArrayEquals(character.toCharArray(), symbolToken.getSymbol().getSymbolValue());
        });
    }
}