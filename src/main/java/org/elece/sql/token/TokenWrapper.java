package org.elece.sql.token;

import org.elece.exception.DbError;
import org.elece.exception.sql.TokenizerException;
import org.elece.sql.token.model.Token;

import java.util.Objects;

public class TokenWrapper {
    private final Token token;
    private final DbError error;

    private TokenWrapper(Builder builder) {
        this.token = builder.token;
        this.error = builder.error;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Token token;
        private DbError error;

        public Builder token(Token token) {
            this.token = token;
            return this;
        }

        public Builder error(DbError error) {
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

    public DbError getError() {
        return error;
    }

    public boolean hasToken() {
        return !Objects.isNull(token) && Objects.isNull(error);
    }

    public boolean hasError() {
        return !Objects.isNull(error);
    }

    public Token unwrap() throws TokenizerException {
        if (hasError()) {
            throw new TokenizerException(getError());
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
