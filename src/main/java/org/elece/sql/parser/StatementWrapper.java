package org.elece.sql.parser;

import org.elece.sql.parser.error.StatementError;
import org.elece.sql.parser.statement.Statement;

import java.util.Objects;

public class StatementWrapper {
    private final Statement statement;
    private final StatementError error;

    private StatementWrapper(StatementWrapper.Builder builder) {
        this.statement = builder.statement;
        this.error = builder.error;
    }

    public static StatementWrapper.Builder builder() {
        return new StatementWrapper.Builder();
    }

    public static class Builder {
        private Statement statement;
        private StatementError error;

        public StatementWrapper.Builder statement(Statement statement) {
            this.statement = statement;
            return this;
        }

        public StatementWrapper.Builder error(StatementError error) {
            this.error = error;
            return this;
        }

        public boolean hasStatement() {
            return !Objects.isNull(statement);
        }

        public StatementWrapper build() {
            return new StatementWrapper(this);
        }
    }

    public Statement getStatement() {
        return statement;
    }

    public StatementError getError() {
        return error;
    }

    public boolean hasStatement() {
        return !Objects.isNull(statement) && Objects.isNull(error);
    }

    public boolean hasError() {
        return !Objects.isNull(error);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatementWrapper that = (StatementWrapper) o;
        return Objects.equals(getStatement(), that.getStatement()) && Objects.equals(getError(), that.getError());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStatement(), getError());
    }


}
