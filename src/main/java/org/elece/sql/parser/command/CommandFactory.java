package org.elece.sql.parser.command;

import org.elece.sql.token.PeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.type.Keyword;

public class CommandFactory {
    public KeywordCommand buildCommand(Keyword keyword, PeekableIterator<TokenWrapper> tokenizer) {
        return switch (keyword) {
            case SELECT -> new SelectKeywordCommand(tokenizer);
            case CREATE -> new CreateKeywordCommand(tokenizer);
            case UPDATE -> new UpdateKeywordCommand(tokenizer);
            case INSERT -> new InsertKeywordCommand(tokenizer);
            case DELETE -> new DeleteKeywordCommand(tokenizer);
            case DROP -> new DropKeywordCommand(tokenizer);
            default -> null;
        };
    }
}
