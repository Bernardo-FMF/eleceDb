package org.elece.sql.parser.command;

import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.parser.statement.DropDbStatement;
import org.elece.sql.parser.statement.DropTableStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.PeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

public class DropKeywordCommand extends AbstractKeywordCommand {
    public DropKeywordCommand(PeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        KeywordToken nextToken = (KeywordToken) expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken &&
                (((KeywordToken) token).getKeyword() == Keyword.Table ||
                        ((KeywordToken) token).getKeyword() == Keyword.Database));

        String identifier = parseIdentifier();

        return nextToken.getKeyword() == Keyword.Table ? new DropTableStatement(identifier) : new DropDbStatement(identifier);
    }
}
