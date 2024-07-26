package org.elece.sql.parser.command;

import org.elece.sql.parser.StatementWrapper;
import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.statement.DropDbStatement;
import org.elece.sql.parser.statement.DropTableStatement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

public class DropKeywordCommand extends AbstractKeywordCommand {
    public DropKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public StatementWrapper parse() throws SqlException, TokenizerException {
        KeywordToken nextToken = (KeywordToken) expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken &&
                (((KeywordToken) token).getKeyword() == Keyword.Table ||
                        ((KeywordToken) token).getKeyword() == Keyword.Database));

        String identifier = parseIdentifier();

        return StatementWrapper.builder().statement(nextToken.getKeyword() == Keyword.Table ? new DropTableStatement(identifier) : new DropDbStatement(identifier)).build();
    }
}
