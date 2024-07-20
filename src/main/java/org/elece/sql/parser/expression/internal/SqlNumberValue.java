package org.elece.sql.parser.expression.internal;

public class SqlNumberValue extends SqlValue<Long> {
    public SqlNumberValue(Long value) {
        super(value);
    }

    @Override
    protected Integer compare(SqlValue<Long> target) {
        return Long.compare(this.getValue(), target.getValue());
    }
}
