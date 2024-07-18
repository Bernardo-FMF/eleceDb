package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.NumberToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NumberTokenProcessorTest {
    @Test
    public void test_number() {
        NumberTokenProcessor numberTokenProcessor = new NumberTokenProcessor();

        String input = "1234 ";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(numberTokenProcessor.matches(charStream.peek()));
        NumberToken token = numberTokenProcessor.consume(charStream);
        Assertions.assertEquals("1234", token.getNumber());
    }
}