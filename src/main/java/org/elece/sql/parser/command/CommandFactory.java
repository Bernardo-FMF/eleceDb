package org.elece.sql.parser.command;

import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.type.Keyword;

public class CommandFactory {
    public IKeywordCommand buildCommand(Keyword keyword, IPeekableIterator<TokenWrapper> tokenizer) {
        return switch (keyword) {
            case Select -> new SelectKeywordCommand(tokenizer);
            case Create -> new CreateKeywordCommand(tokenizer);
            default -> null;
        };
    }
}
