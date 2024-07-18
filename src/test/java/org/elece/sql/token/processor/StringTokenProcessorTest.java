package org.elece.sql.token.processor;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.model.StringToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StringTokenProcessorTest {
    @Test
    public void test_singleQuotationMarks() {
        StringTokenProcessor stringTokenProcessor = new StringTokenProcessor();

        String input = "'test'";
        CharStream charStream = new CharStream(input);

            Assertions.assertTrue(stringTokenProcessor.matches(input.charAt(0)));
            StringToken token = stringTokenProcessor.consume(charStream);
            Assertions.assertEquals("test", token.getString());
    }

    @Test
    public void test_doubleQuotationMarks() {
        StringTokenProcessor stringTokenProcessor = new StringTokenProcessor();

        String input = "\"test\"";
        CharStream charStream = new CharStream(input);

        Assertions.assertTrue(stringTokenProcessor.matches(input.charAt(0)));
        StringToken token = stringTokenProcessor.consume(charStream);
        Assertions.assertEquals("test", token.getString());
    }
}