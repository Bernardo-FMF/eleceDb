package org.elece.sql.token;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.elece.sql.token.error.TokenError;
import org.elece.sql.token.model.Token;

import java.util.Objects;
import java.util.Optional;

public class TokenWrapper {
    @Nullable
    private final Token token;
    @Nonnull
    private final Location location;
    @Nullable
    private final TokenError error;

    public TokenWrapper(@Nullable Token token, @Nullable TokenError error, @Nonnull Location location) {
        this.token = token;
        this.error = error;
        this.location = location;
    }

    public Optional<Token> getToken() {
        return Optional.ofNullable(token);
    }

    public boolean hasToken() {
        return !Objects.isNull(token) && Objects.isNull(error);
    }

    public Location getLocation() {
        return location;
    }
}
