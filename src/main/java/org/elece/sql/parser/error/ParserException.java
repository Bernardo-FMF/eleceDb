package org.elece.sql.parser.error;

public class ParserException extends Exception {
    public ParserException(StatementError error) {
        super(error.format());
    }
}
