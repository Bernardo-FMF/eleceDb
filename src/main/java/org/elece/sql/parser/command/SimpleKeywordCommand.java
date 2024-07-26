package org.elece.sql.parser.command;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.error.UnspecifiedError;
import org.elece.sql.parser.statement.CommitStatement;
import org.elece.sql.parser.statement.RollbackStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.type.Keyword;

public class SimpleKeywordCommand extends AbstractKeywordCommand {
    private final Keyword keyword;

    public SimpleKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer, Keyword keyword) {
        super(tokenizer);
        this.keyword = keyword;
    }

    @Override
    public Statement parse() throws SqlException {
        return switch (keyword) {
            case Commit -> new CommitStatement();
            case Rollback -> new RollbackStatement();
            default -> throw new SqlException(new UnspecifiedError("Unknown query"));
        };
    }
}
