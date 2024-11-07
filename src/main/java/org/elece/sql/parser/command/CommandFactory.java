package org.elece.sql.parser.command;

import org.elece.sql.token.PeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.type.Keyword;

public class CommandFactory {
    public KeywordCommand buildCommand(Keyword keyword, PeekableIterator<TokenWrapper> tokenizer) {
        return switch (keyword) {
            case Select -> new SelectKeywordCommand(tokenizer);
            case Create -> new CreateKeywordCommand(tokenizer);
            case Update -> new UpdateKeywordCommand(tokenizer);
            case Insert -> new InsertKeywordCommand(tokenizer);
            case Delete -> new DeleteKeywordCommand(tokenizer);
            case Start -> new StartKeywordCommand(tokenizer);
            case Drop -> new DropKeywordCommand(tokenizer);
            case Commit, Rollback -> new SimpleKeywordCommand(tokenizer, keyword);
            default -> null;
        };
    }
}
