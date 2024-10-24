package org.elece.sql.parser.expression.internal;

public class SqlBoolValue extends SqlValue<Boolean> {
    public SqlBoolValue(Boolean value) {
        super(value);
    }

    @Override
    public Integer compare(SqlValue<Boolean> target) {
        return Boolean.compare(this.getValue(), target.getValue());
    }
}
