package org.elece.sql.parser.expression.internal;

import java.math.BigInteger;

public class SqlNumberValue extends SqlValue<BigInteger> {
    public SqlNumberValue(BigInteger value) {
        super(value);
    }

    @Override
    protected Integer compare(SqlValue<BigInteger> target) {
        return getValue().compareTo(target.getValue());
    }
}
