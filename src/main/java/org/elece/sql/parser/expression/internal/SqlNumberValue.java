package org.elece.sql.parser.expression.internal;

import java.math.BigInteger;

public class SqlNumberValue extends SqlValue<Integer> {
    public SqlNumberValue(Integer value) {
        super(value);
    }

    @Override
    protected Integer compare(SqlValue<Integer> target) {
        return getValue().compareTo(target.getValue());
    }
}
