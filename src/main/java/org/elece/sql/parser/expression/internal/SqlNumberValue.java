package org.elece.sql.parser.expression.internal;

public class SqlNumberValue extends SqlValue<Integer> {
    public SqlNumberValue(Integer value) {
        super(value);
    }

    @Override
    public Integer compare(SqlValue<Integer> target) {
        return getValue().compareTo(target.getValue());
    }
}
