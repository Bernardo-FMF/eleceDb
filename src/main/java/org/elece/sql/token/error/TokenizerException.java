package org.elece.sql.token.error;

public class TokenizerException extends Exception {
    public TokenizerException(TokenError error) {
        super(error.format());
    }
}
