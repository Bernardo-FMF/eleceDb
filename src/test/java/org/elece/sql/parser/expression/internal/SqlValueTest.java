package org.elece.sql.parser.expression.internal;

import org.elece.sql.parser.error.ParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

class SqlValueTest {
    @Test
    public void test_number_equalValueTypes_equalValues() throws ParserException {
        SqlNumberValue sourceValue = new SqlNumberValue(new BigInteger("1"));
        SqlNumberValue targetValue = new SqlNumberValue(new BigInteger("1"));
        Assertions.assertEquals(0, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_number_equalValueTypes_differentValues() throws ParserException {
        SqlNumberValue sourceValue = new SqlNumberValue(new BigInteger("1"));
        SqlNumberValue targetValue = new SqlNumberValue(new BigInteger("2"));
        Assertions.assertEquals(-1, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_string_equalValueTypes_equalValues() throws ParserException {
        SqlStringValue sourceValue = new SqlStringValue("test");
        SqlStringValue targetValue = new SqlStringValue("test");
        Assertions.assertEquals(0, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_string_equalValueTypes_differentValues() throws ParserException {
        SqlStringValue sourceValue = new SqlStringValue("test");
        SqlStringValue targetValue = new SqlStringValue("test2");
        Assertions.assertEquals(-1, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_boolean_equalValueTypes_equalValues() throws ParserException {
        SqlBoolValue sourceValue = new SqlBoolValue(false);
        SqlBoolValue targetValue = new SqlBoolValue(false);
        Assertions.assertEquals(0, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_boolean_equalValueTypes_differentValues() throws ParserException {
        SqlBoolValue sourceValue = new SqlBoolValue(false);
        SqlBoolValue targetValue = new SqlBoolValue(true);
        Assertions.assertEquals(-1, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_differentValueTypes() {
        SqlBoolValue sourceValue = new SqlBoolValue(false);
        SqlStringValue targetValue = new SqlStringValue("test");
        Assertions.assertThrows(ParserException.class, () -> sourceValue.partialComparison(targetValue));
    }
}