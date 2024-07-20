package org.elece.sql.token.error;

public class TokenizerException extends Exception {
    private final TokenError error;

    public TokenizerException(TokenError error) {
        super(error.format());
        this.error = error;
    }

    public TokenError getError() {
        return error;
    }
}
