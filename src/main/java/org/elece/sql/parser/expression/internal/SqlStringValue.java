package org.elece.sql.parser.expression.internal;

public class SqlStringValue extends SqlValue<String> {
    public SqlStringValue(String value) {
        super(value);
    }

    @Override
    public Integer compare(SqlValue<String> target) {
        return this.getValue().compareTo(target.getValue());
    }
}
