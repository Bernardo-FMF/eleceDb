package org.elece.sql.parser.command;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.type.parser.UnspecifiedError;
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
    public Statement parse() throws ParserException {
        return switch (keyword) {
            case Commit -> new CommitStatement();
            case Rollback -> new RollbackStatement();
            default -> throw new ParserException(new UnspecifiedError("Unknown query"));
        };
    }
}
