package org.elece.sql.parser.error;

public class SqlException extends Exception {
    public SqlException(StatementError error) {
        super(error.format());
    }
}
