package org.elece.sql.token;

import org.elece.sql.token.error.TokenError;
import org.elece.sql.token.model.Token;

import java.util.Objects;

public class TokenWrapper {
    private final Token token;
    private final TokenError error;

    private TokenWrapper(Builder builder) {
        this.token = builder.token;
        this.error = builder.error;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Token token;
        private TokenError error;

        public Builder token(Token token) {
            this.token = token;
            return this;
        }

        public Builder error(TokenError error) {
            this.error = error;
            return this;
        }

        public boolean hasToken() {
            return !Objects.isNull(token);
        }

        public TokenWrapper build() {
            return new TokenWrapper(this);
        }
    }

    public Token getToken() {
        return token;
    }

    public TokenError getError() {
        return error;
    }

    public boolean hasToken() {
        return !Objects.isNull(token) && Objects.isNull(error);
    }

    public boolean hasError() {
        return !Objects.isNull(error);
    }
}
