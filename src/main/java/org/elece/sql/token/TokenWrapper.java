package org.elece.sql.token;

import org.elece.exception.DbError;
import org.elece.exception.TokenizerException;
import org.elece.sql.token.model.Token;

import java.util.Objects;

public class TokenWrapper {
    private final Token token;
    private final DbError error;
    private final String message;

    private TokenWrapper(Builder builder) {
        this.token = builder.token;
        this.error = builder.error;
        this.message = builder.message;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Token token;
        private DbError error;
        private String message;

        public Builder setToken(Token token) {
            this.token = token;
            return this;
        }

        public Builder setError(DbError dbError, String message) {
            this.error = dbError;
            this.message = message;
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

    public DbError getError() {
        return error;
    }

    public String getMessage() {
        return message;
    }

    public boolean hasToken() {
        return !Objects.isNull(token) && Objects.isNull(error);
    }

    public boolean hasError() {
        return !Objects.isNull(error);
    }

    public Token unwrap() throws TokenizerException {
        if (hasError()) {
            throw new TokenizerException(getError(), getMessage());
        }
        return token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenWrapper that = (TokenWrapper) o;
        return Objects.equals(getToken(), that.getToken()) && Objects.equals(getError(), that.getError());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getToken(), getError());
    }
}
