package org.elece.sql.parser.expression.internal;

import org.elece.sql.parser.error.SqlException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlValueTest {
    @Test
    public void test_number_equalValueTypes_equalValues() throws SqlException {
        SqlNumberValue sourceValue = new SqlNumberValue(1L);
        SqlNumberValue targetValue = new SqlNumberValue(1L);
        Assertions.assertEquals(0, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_number_equalValueTypes_differentValues() throws SqlException {
        SqlNumberValue sourceValue = new SqlNumberValue(1L);
        SqlNumberValue targetValue = new SqlNumberValue(2L);
        Assertions.assertEquals(-1, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_string_equalValueTypes_equalValues() throws SqlException {
        SqlStringValue sourceValue = new SqlStringValue("test");
        SqlStringValue targetValue = new SqlStringValue("test");
        Assertions.assertEquals(0, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_string_equalValueTypes_differentValues() throws SqlException {
        SqlStringValue sourceValue = new SqlStringValue("test");
        SqlStringValue targetValue = new SqlStringValue("test2");
        Assertions.assertEquals(-1, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_boolean_equalValueTypes_equalValues() throws SqlException {
        SqlBoolValue sourceValue = new SqlBoolValue(false);
        SqlBoolValue targetValue = new SqlBoolValue(false);
        Assertions.assertEquals(0, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_boolean_equalValueTypes_differentValues() throws SqlException {
        SqlBoolValue sourceValue = new SqlBoolValue(false);
        SqlBoolValue targetValue = new SqlBoolValue(true);
        Assertions.assertEquals(-1, sourceValue.partialComparison(targetValue));
    }

    @Test
    public void test_differentValueTypes() {
        SqlBoolValue sourceValue = new SqlBoolValue(false);
        SqlStringValue targetValue = new SqlStringValue("test");
        Assertions.assertThrows(SqlException.class, () -> sourceValue.partialComparison(targetValue));
    }
}